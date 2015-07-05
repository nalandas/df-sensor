package database

import (
    "github.com/nalandras/data-forwarder/util"
    "io/ioutil"
    "os"
    "path"
    "reflect"
    "testing"
)

func makeTempDir(t *testing.T) string {
    tmpdir, err := ioutil.TempDir("", "logstash-config-test")
    util.Chkerr(t, err)
    return tmpdir
}

func rmTempDir(tmpdir string) {
    _ = os.RemoveAll(tmpdir)
}

// -------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------
func TestLoadConfig(t *testing.T) {
    configJson1 := `
{
# A comment at the beginning of the line
  "databases": [
    {
      "db_type": "mysql",
      "connection_url": "tcp(127.0.0.1:3306)/dbname?charset=utf8",
      "db_user": "username",
      "db_pass": "password",
      "tables": [
        {
          "table_name": "table1",
          "fields": ["col1", "col2"],
          "incremental": 10,
          "status_path": "/tmp/",
          "custom_query": "select * from table1 where something",
          "query_delay" : 50,
          "max_rows": 2000
        }, {
           "table_name": "table2"
        }
      ]
    }
  ]
}`

    configJson2 := `
{
# A comment at the beginning of the line
  "databases": [
    {
      "db_type": "mysql",
      "connection_url": "tcp(127.0.0.1:3306)/dbname1?charset=utf8",
      "db_user": "user1",
      "db_pass": "pass1",
      "tables": [
        {
          "table_name": "tab1",
          "fields": ["col", "col1"],
          "incremental": 10,
          "status_path": "/tmp/mysql1",
          "query_delay" : 40,
          "max_rows": 1000
        }
      ]
    }
  ]
}`
    tmpdir := makeTempDir(t)
    defer rmTempDir(tmpdir)

    configFile := path.Join(tmpdir, "myconfig1")
    err := ioutil.WriteFile(configFile, []byte(configJson1), 0644)
    util.Chkerr(t, err)

    configFile = path.Join(tmpdir, "myconfig2")
    err = ioutil.WriteFile(configFile, []byte(configJson2), 0644)
    util.Chkerr(t, err)

    dbsensor := new(DBSensor)
    err = dbsensor.LoadConfig(tmpdir)

    expected := DBConfig{
        Databases: []Databases{{
            DBType:        "mysql",
            ConnectionURL: "tcp(127.0.0.1:3306)/dbname?charset=utf8",
            DBUser:        "username",
            DBPass:        "password",
            Tables: []TableConfig{{
                TableName:   "table1",
                Fields:      []string{"col1", "col2"},
                Incremental: 10,
                StatusPath:  "/tmp/",
                CustomQuery: "select * from table1 where something",
                QueryDelay:  50,
                MaxRows:     2000,
            }, {
                TableName:   "table2",
                Fields:      defaultConfig.fields,
                Incremental: defaultConfig.incremental,
                StatusPath:  defaultConfig.statusPath,
                CustomQuery: "",
                QueryDelay:  defaultConfig.queryDaly,
                MaxRows:     defaultConfig.maxRows,
            }},
        }, {
            DBType:        "mysql",
            ConnectionURL: "tcp(127.0.0.1:3306)/dbname1?charset=utf8",
            DBUser:        "user1",
            DBPass:        "pass1",
            Tables: []TableConfig{{
                TableName:   "tab1",
                Fields:      []string{"col", "col1"},
                Incremental: 10,
                StatusPath:  "/tmp/mysql1",
                QueryDelay:  40,
                MaxRows:     1000,
            }},
        }},
    }
    if !reflect.DeepEqual(expected, dbsensor.DriverConfig) {
        t.Fatalf("Expected\n%v\n\ngot\n\n%v\n\nfrom LoadConfig, err", expected, dbsensor.DriverConfig)
    }
}
