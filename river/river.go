package river

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/canal"
)

// ErrRuleNotExist is the error if rule is not defined.
var ErrRuleNotExist = errors.New("rule is not exist")

// River is a pluggable service within Elasticsearch pulling data then indexing it into Elasticsearch.
// We use this definition here too, although it may not run within Elasticsearch.
// Maybe later I can implement a acutal river in Elasticsearch, but I must learn java. :-)
type River struct {
	c *Config

	canal *canal.Canal

	rules map[string]*Rule

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	es *elastic.Client

	master *masterInfo

	syncCh chan interface{}
}

// NewRiver creates the River from config
func NewRiver(c *Config) (*River, error) {
	r := new(River)

	//配置
	r.c = c
	//初始化rules
	r.rules = make(map[string]*Rule)
	//初始化syncCh
	r.syncCh = make(chan interface{}, 4096)
	//初始化ctx cancel
	r.ctx, r.cancel = context.WithCancel(context.Background())

	var err error
	//从配置的目录加载master数据，主要包含 bin_name 、bin_pos、filePath 、lastSaveTime
	if r.master, err = loadMasterInfo(c.DataDir); err != nil {
		return nil, errors.Trace(err)
	}

	//创建canal
	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	//准备rule, 格式: r.rules[ruleKey(schema, table)] = rule{}
	if err = r.prepareRule(); err != nil {
		return nil, errors.Trace(err)
	}

	//准备canal,设置SetEventHandler
	if err = r.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}

	//初始化esClient
	cfg := new(elastic.ClientConfig)
	cfg.Addr = r.c.ESAddr
	cfg.User = r.c.ESUser
	cfg.Password = r.c.ESPassword
	cfg.HTTPS = r.c.ESHttps
	//设置esClient
	r.es = elastic.NewClient(cfg)

	//prometheus 指标
	go InitStatus(r.c.StatAddr, r.c.StatPath)

	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.MyAddr
	cfg.User = r.c.MyUser
	cfg.Password = r.c.MyPassword
	cfg.Charset = r.c.MyCharset
	//flavor is mysql or mariadb
	cfg.Flavor = r.c.Flavor

	//
	cfg.ServerID = r.c.ServerID
	cfg.Dump.ExecutionPath = r.c.DumpExec
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = r.c.SkipMasterData

	//设置来自Sources字段的正则过滤 db+table
	for _, s := range r.c.Sources {
		for _, t := range s.Tables {
			cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, s.Schema+"\\."+t)
		}
	}

	var err error
	//设置canal
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *River) prepareCanal() error {
	var db string
	dbs := map[string]struct{}{}
	tables := make([]string, 0, len(r.rules))
	for _, rule := range r.rules {
		db = rule.Schema
		dbs[rule.Schema] = struct{}{}
		tables = append(tables, rule.Table)
	}

	//
	if len(dbs) == 1 {
		// one db, we can shrink using table
		r.canal.AddDumpTables(db, tables...)
	} else {
		// many dbs, can only assign databases to dump
		keys := make([]string, 0, len(dbs))
		for key := range dbs {
			keys = append(keys, key)
		}

		r.canal.AddDumpDatabases(keys...)
	}

	//SetEventHandler
	r.canal.SetEventHandler(&eventHandler{r})

	return nil
}

func (r *River) newRule(schema, table string) error {
	key := ruleKey(schema, table)

	if _, ok := r.rules[key]; ok {
		return errors.Errorf("duplicate source %s, %s defined in config", schema, table)
	}

	r.rules[key] = newDefaultRule(schema, table)
	return nil
}

//表更新时候更新rule
func (r *River) updateRule(schema, table string) error {
	rule, ok := r.rules[ruleKey(schema, table)]
	if !ok {
		return ErrRuleNotExist
	}

	tableInfo, err := r.canal.GetTable(schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	rule.TableInfo = tableInfo

	return nil
}

/**
	解析source，根据配置的表名生成默认的rule规则，
	Sources: [] river.SourceConfig {
        river.SourceConfig {
            Schema: "test",
            Tables: [] string {
                "t",
                "t_[0-9]{4}",
                "tfield",
                "tfilter"
            }
        }
    }
*/
func (r *River) parseSource() (map[string][]string, error) {
	//初始化通配符表wildtables, 默认为1个数据源

	wildTables := make(map[string][]string, len(r.c.Sources))

	// first, check sources
	for _, s := range r.c.Sources {
		//判断表名称是否可用，* 只允许出现在 len(s.Tables)==1 情况下，大于1时不允许
		if !isValidTables(s.Tables) {
			return nil, errors.Errorf("wildcard * is not allowed for multiple tables")
		}

		for _, table := range s.Tables {
			//issue 这快应该可以放在外面，避免重复判断
			if len(s.Schema) == 0 {
				return nil, errors.Errorf("empty schema not allowed for source")
			}

			//QuoteMeta返回将s中所有正则表达式元字符都进行转义后字符串。该字符串可以用在正则表达式中匹配字面值s。
			//例如，QuoteMeta(`[foo]`)会返回`\[foo\]`。
			//表名使用正则表达式
			if regexp.QuoteMeta(table) != table {
				//ruleKey格式化，判断db：table是否重复定义，此处table为正则格式
				if _, ok := wildTables[ruleKey(s.Schema, table)]; ok {
					return nil, errors.Errorf("duplicate wildcard table defined for %s.%s", s.Schema, table)
				}

				tables := []string{}

				//通过 RLIKE 正则查询表名 ?
				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
					table_name RLIKE "%s" AND table_schema = "%s";`, buildTable(table), s.Schema)

				res, err := r.canal.Execute(sql)
				if err != nil {
					return nil, errors.Trace(err)
				}

				for i := 0; i < res.Resultset.RowNumber(); i++ {
					f, _ := res.GetString(i, 0)
					//根据schema,table 创建默认rule
					err := r.newRule(s.Schema, f)
					if err != nil {
						return nil, errors.Trace(err)
					}
					//查询结果append到tables切片
					tables = append(tables, f)
				}
				//{"test:t_[0-9]{4}":["t_0000","t_1002"]} //
				wildTables[ruleKey(s.Schema, table)] = tables
			} else {
				//否则直接创建rule规则，newDefaultRule只初始化了部分字段，如Schema,Table,Index,Type及空的FieldMapping
				//r.rules[ruleKey(s.Schema, table] = newDefaultRule(schema, table)
				err := r.newRule(s.Schema, table)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}

	//判断rules不为空
	if len(r.rules) == 0 {
		return nil, errors.Errorf("no source data defined")
	}

	return wildTables, nil
}

func (r *River) prepareRule() error {
	//根据配置的正则获得通配符表
	wildtables, err := r.parseSource()
	if err != nil {
		return errors.Trace(err)
	}

	//如果Rules配置项不为空，为空则走parseSource的默认规则
	if r.c.Rules != nil {
		// then, set custom mapping rule
		for _, rule := range r.c.Rules {
			if len(rule.Schema) == 0 {
				return errors.Errorf("empty schema not allowed for rule")
			}
			//通配符表
			if regexp.QuoteMeta(rule.Table) != rule.Table {
				//判断rule规则的db+table是否配置了source属性
				tables, ok := wildtables[ruleKey(rule.Schema, rule.Table)]
				if !ok {
					return errors.Errorf("wildcard table for %s.%s is not defined in source", rule.Schema, rule.Table)
				}
				//index不能为空
				if len(rule.Index) == 0 {
					return errors.Errorf("wildcard table rule %s.%s must have a index, can not empty", rule.Schema, rule.Table)
				}
				//规则准备，比如index、type转小写，初始化FieldMapping
				rule.prepare()

				//遍历通过db正则查询的表，根据配置实例化单个rule规则, 没有设置 Filter，PipeLine
				for _, table := range tables {
					rr := r.rules[ruleKey(rule.Schema, table)]
					rr.Index = rule.Index
					rr.Type = rule.Type
					rr.Parent = rule.Parent
					rr.ID = rule.ID
					rr.FieldMapping = rule.FieldMapping
				}
			} else {
				key := ruleKey(rule.Schema, rule.Table)
				if _, ok := r.rules[key]; !ok {
					return errors.Errorf("rule %s, %s not defined in source", rule.Schema, rule.Table)
				}
				rule.prepare()
				//使用当前rule替换newDefaultRule
				r.rules[key] = rule
			}
		}
	}

	rules := make(map[string]*Rule)
	for key, rule := range r.rules {
		//获得rule.TableInfo
		if rule.TableInfo, err = r.canal.GetTable(rule.Schema, rule.Table); err != nil {
			return errors.Trace(err)
		}

		if len(rule.TableInfo.PKColumns) == 0 {
			//如果表没有主键，但是没有配置跳过主键
			if !r.c.SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}

			log.Errorf("ignored table without a primary key: %s\n", rule.TableInfo.Name)
		} else {
			rules[key] = rule
		}
	}
	//
	r.rules = rules

	return nil
}

func ruleKey(schema string, table string) string {
	return strings.ToLower(fmt.Sprintf("%s:%s", schema, table))
}

// Run syncs the data from MySQL and inserts to ES.
func (r *River) Run() error {
	r.wg.Add(1)
	canalSyncState.Set(float64(1))
	//for循环处理同步业务
	go r.syncLoop()

	//获取master的同步点
	pos := r.master.Position()
	//通过RunFrom启动canal服务
	if err := r.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err %v", err)
		canalSyncState.Set(0)
		return errors.Trace(err)
	}

	return nil
}

// Ctx returns the internal context for outside use.
func (r *River) Ctx() context.Context {
	return r.ctx
}

// Close closes the River
func (r *River) Close() {
	log.Infof("closing river")

	r.cancel()

	r.canal.Close()

	r.master.Close()

	//当等待组计数器不等于 0 时阻塞，直到变 0
	//syncLoop()方法 defer 处执行 r.wg.Done()
	r.wg.Wait()
}

func isValidTables(tables []string) bool {
	if len(tables) > 1 {
		for _, table := range tables {
			if table == "*" {
				return false
			}
		}
	}
	return true
}

//对*做处理
func buildTable(table string) string {
	if table == "*" {
		return "." + table
	}
	return table
}
