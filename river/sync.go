package river

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

const (
	//定义常量，转换mysql字段类型为list，或把时间戳转成日期时间型
	fieldTypeList = "list"
	// for the mysql int type to es date type
	// set the [rule.field] created_time = ",date"
	fieldTypeDate = "date"
)

const mysqlDateFormat = "2006-01-02"

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r *River
}

//binlog切割 返回r.ctx.Err()
func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}
	//强制保存Position
	h.r.syncCh <- posSaver{pos, true}

	return h.r.ctx.Err()
}

//表修改时更改rule.TableInfo
func (h *eventHandler) OnTableChanged(schema, table string) error {
	err := h.r.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
		return errors.Trace(err)
	}
	return nil
}

//DDL操作强制保存Position
func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.r.syncCh <- posSaver{nextPos, true}
	return h.r.ctx.Err()
}

//XID_EVENT一个事务的提交操作，保存Position,非强制
func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.r.syncCh <- posSaver{nextPos, false}
	return h.r.ctx.Err()
}

//有binlog记录时
func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	//根据db+table获取rule规则
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}

	var reqs []*elastic.BulkRequest
	var err error
	switch e.Action {
	case canal.InsertAction:
		//插入
		reqs, err = h.r.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		//删除
		reqs, err = h.r.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		//更新
		reqs, err = h.r.makeUpdateRequest(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		//如果有错误,执行context.CancelFunc
		h.r.cancel()
		return errors.Errorf("make %s ES request err %v, close sync", e.Action, err)
	}

	h.r.syncCh <- reqs

	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (h *eventHandler) String() string {
	return "ESRiverEventHandler"
}

func (r *River) syncLoop() {
	//批处理数据大小
	bulkSize := r.c.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := r.c.FlushBulkTime.Duration
	if interval == 0 {
		//刷新定时器
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)

	//退出时执行的defer
	defer ticker.Stop()
	defer r.wg.Done()

	//上次保存同步点时间
	lastSavedTime := time.Now()
	//elastic.BulkRequest 切片
	reqs := make([]*elastic.BulkRequest, 0, 1024)

	var pos mysql.Position

	for {
		//
		needFlush := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			//保存pos点
			case posSaver:
				now := time.Now()
				//强制执行或者上次保存时间距当前时间3秒以上
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					//更新上次保存时间,及是否需要刷新、保存标记
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case []*elastic.BulkRequest:
				//追加到reqs切片
				reqs = append(reqs, v...)
				//根据切片长度是否大于buldSize设置needFlush标记
				needFlush = len(reqs) >= bulkSize
			}
		case <-ticker.C:
			//定时设置needFlush标记为true，处理es请求
			needFlush = true
		case <-r.ctx.Done():
			//跳出循环，返回
			return
		}

		if needFlush {
			// TODO: retry some times?
			if err := r.doBulk(reqs); err != nil {
				log.Errorf("do ES bulk err %v, close sync", err)
				//出错执行r.cancel()
				r.cancel()
				return
			}
			//重置reqs切片为空
			reqs = reqs[0:0]
		}

		//保存pos点
		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				r.cancel()
				return
			}
		}
	}
}

// for insert and delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for _, values := range rows {
		//获取id
		id, err := r.getDocID(rule, values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		parentID := ""
		if len(rule.Parent) > 0 {
			//获取parentId， rule.Parent 为字段名
			if parentID, err = r.getParentID(rule, values, rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: id, Parent: parentID, Pipeline: rule.Pipeline}

		if action == canal.DeleteAction {
			//删除动作
			req.Action = elastic.ActionDelete
			esDeleteNum.WithLabelValues(rule.Index).Inc()
		} else {
			//插入动作，创建插入请求数据，填充req.data字段
			r.makeInsertReqData(req, rule, values)
			esInsertNum.WithLabelValues(rule.Index).Inc()
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

//insertRequest和DeleteRequest都请求makeRequest方法
//只是insert需要insertRequestData数据，又调用了makeInsertReqData方法，delete直接删除就行
//创建插入数据请求
func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

//创建删除数据请求
func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

//创建更新数据请求
func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	//[[before][after]]
	//更新操作，需要前后数据，因此长度是2的倍数
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {

		beforeID, err := r.getDocID(rule, rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}

		afterID, err := r.getDocID(rule, rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}

		beforeParentID, afterParentID := "", ""
		if len(rule.Parent) > 0 {
			if beforeParentID, err = r.getParentID(rule, rows[i], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
			if afterParentID, err = r.getParentID(rule, rows[i+1], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: beforeID, Parent: beforeParentID}

		//如果更新了主键值，则是一次插入删除操作
		if beforeID != afterID || beforeParentID != afterParentID {
			//删除操作
			req.Action = elastic.ActionDelete
			reqs = append(reqs, req)

			//插入操作，生成数据
			req = &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: afterID, Parent: afterParentID, Pipeline: rule.Pipeline}
			r.makeInsertReqData(req, rule, rows[i+1])

			esDeleteNum.WithLabelValues(rule.Index).Inc()
			esInsertNum.WithLabelValues(rule.Index).Inc()
		} else {
			//如果配置了pipeline
			if len(rule.Pipeline) > 0 {
				// Pipelines can only be specified on index action
				r.makeInsertReqData(req, rule, rows[i+1])
				// Make sure action is index, not create
				req.Action = elastic.ActionIndex
				req.Pipeline = rule.Pipeline
			} else {
				//根据req,rule,befor,after生成更新请求数据
				r.makeUpdateReqData(req, rule, rows[i], rows[i+1])
			}
			esUpdateNum.WithLabelValues(rule.Index).Inc()
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

//根据列、和value接口获取value转换后的值
func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	//字符串类型
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	//日期时间型
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(time.RFC3339)
		}
	//日期型
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(mysqlDateFormat, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(mysqlDateFormat)
		}
	}

	return value
}

//处理keywords=",list"这种格式, k=keywords v=",list"
//返回mysql字段, elastic字段, fieldType字段类型
func (r *River) getFieldParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	elastic := composedField[0]
	fieldType := ""

	if 0 == len(elastic) {
		elastic = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, elastic, fieldType
}

//生成插入请求数据
func (r *River) makeInsertReqData(req *elastic.BulkRequest, rule *Rule, values []interface{}) {
	req.Data = make(map[string]interface{}, len(values))
	//如果数据存在，使用create操作失败，会提示文档已经存在，使用index则可以成功执行。
	req.Action = elastic.ActionIndex

	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			//跳过过滤的字段
			continue
		}
		mapped := false
		for k, v := range rule.FieldMapping {
			mysql, elastic, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				//字段映射
				mapped = true
				//字段转换，如转数组、日期时间，或者执行makeReqColumnData(&c, values[i])获取value值
				req.Data[elastic] = r.getFieldValue(&c, fieldType, values[i])
			}
		}
		if mapped == false {
			//makeReqColumnData 直接根据列、和value接口获取转换后的值
			req.Data[c.Name] = r.makeReqColumnData(&c, values[i])
		}
	}
}

//生成更新请求数据
func (r *River) makeUpdateReqData(req *elastic.BulkRequest, rule *Rule,
	beforeValues []interface{}, afterValues []interface{}) {
	req.Data = make(map[string]interface{}, len(beforeValues))

	// maybe dangerous if something wrong delete before?
	req.Action = elastic.ActionUpdate

	for i, c := range rule.TableInfo.Columns {
		mapped := false
		if !rule.CheckFilter(c.Name) {
			continue
		}
		//比较更新前后数据是否相同，如果相同跳过
		if reflect.DeepEqual(beforeValues[i], afterValues[i]) {
			//nothing changed
			continue
		}
		//生成请求数据
		for k, v := range rule.FieldMapping {
			mysql, elastic, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				req.Data[elastic] = r.getFieldValue(&c, fieldType, afterValues[i])
			}
		}
		if mapped == false {
			req.Data[c.Name] = r.makeReqColumnData(&c, afterValues[i])
		}

	}
}

// 获取es中的主键，根据TableInfo.GetPKValues生成，或者由rule.Id里的数组组合生成
// If id in toml file is none, get primary keys in one row and format them into a string, and PK must not be nil
// Else get the ID's column in one row and format them into a string
func (r *River) getDocID(rule *Rule, row []interface{}) (string, error) {
	var (
		ids []interface{}
		err error
	)
	if rule.ID == nil {
		ids, err = rule.TableInfo.GetPKValues(row)
		if err != nil {
			return "", err
		}
	} else {
		ids = make([]interface{}, 0, len(rule.ID))
		for _, column := range rule.ID {
			value, err := rule.TableInfo.GetColumnValue(column, row)
			if err != nil {
				return "", err
			}
			ids = append(ids, value)
		}
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range ids {
		if value == nil {
			return "", errors.Errorf("The %ds id or PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
		sep = ":"
	}

	return buf.String(), nil
}

//根据列名获取index，返回row[index]记录里对应的value
func (r *River) getParentID(rule *Rule, row []interface{}, columnName string) (string, error) {
	index := rule.TableInfo.FindColumn(columnName)
	if index < 0 {
		return "", errors.Errorf("parent id not found %s(%s)", rule.TableInfo.Name, columnName)
	}

	return fmt.Sprint(row[index]), nil
}

//使用es客户端批量处理reqs
func (r *River) doBulk(reqs []*elastic.BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}

	if resp, err := r.es.Bulk(reqs); err != nil {
		log.Errorf("sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
		return errors.Trace(err)
	} else if resp.Code/100 == 2 || resp.Errors {
		//如果返回码以2开头，或者resp.Errors为true，遍历resp.Items
		for i := 0; i < len(resp.Items); i++ {
			for action, item := range resp.Items[i] {
				//如果有错误，记录日志
				if len(item.Error) > 0 {
					log.Errorf("%s index: %s, type: %s, id: %s, status: %d, error: %s",
						action, item.Index, item.Type, item.ID, item.Status, item.Error)
				}
			}
		}
	}

	return nil
}

//根据mysql列、es字段类型、row[i]，获取es字段value
//把配置list类型的字符串字段转为使用，切割的切片
//配置日期时间类型的时间戳转换为日期时间
// get mysql field value and convert it to specific value to es
func (r *River) getFieldValue(col *schema.TableColumn, fieldType string, value interface{}) interface{} {
	var fieldValue interface{}
	switch fieldType {
	case fieldTypeList:
		v := r.makeReqColumnData(col, value)
		if str, ok := v.(string); ok {
			fieldValue = strings.Split(str, ",")
		} else {
			fieldValue = v
		}

	case fieldTypeDate:
		if col.Type == schema.TYPE_NUMBER {
			col.Type = schema.TYPE_DATETIME

			v := reflect.ValueOf(value)
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fieldValue = r.makeReqColumnData(col, time.Unix(v.Int(), 0).Format(mysql.TimeFormat))
			}
		}
	}

	if fieldValue == nil {
		fieldValue = r.makeReqColumnData(col, value)
	}
	return fieldValue
}
