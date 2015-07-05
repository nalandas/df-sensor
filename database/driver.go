package database

import (
    "github.com/mitchellh/mapstructure"
    "github.com/nalandras/data-forwarder/sensor"
    "github.com/nalandras/data-forwarder/util"
    "os"
)

type DBSensor struct {
    DriverConfig DBConfig
}

type DBConfig struct {
    Databases []Databases `json:"databases"`
}

type Databases struct {
    DBType        string        `json:"db_type"`
    ConnectionURL string        `json:"connection_url"`
    DBUser        string        `json:"db_user"`
    DBPass        string        `json:"db_pass"`
    Tables        []TableConfig `json:"tables"`
}

type TableConfig struct {
    TableName   string   `json:"table_name"`
    Fields      []string `json:"fields"`
    Incremental int      `json:"incremental"`
    StatusPath  string   `json:"status_path"`
    CustomQuery string   `json:"custom_query"`
    QueryDelay  int      `json:"query_delay"`
    MaxRows     int      `json:"max_rows"`
}

var defaultConfig = &struct {
    fields      []string
    incremental int
    statusPath  string
    queryDaly   int
    maxRows     int
}{
    fields:     []string{"*"},
    statusPath: "/var/lib/sensor/db",
    queryDaly:  10000,
}

func (sens *DBSensor) FinalizeConfig(config *DBConfig) (err error) {
    for i, _ := range config.Databases {
        db := &config.Databases[i]
        for j, _ := range db.Tables {
            table := &db.Tables[j]
            if len(table.Fields) == 0 {
                table.Fields = defaultConfig.fields
            }
            if table.QueryDelay == 0 {
                table.QueryDelay = defaultConfig.queryDaly
            }
            if table.StatusPath == "" {
                table.StatusPath = defaultConfig.statusPath
            }
            if _, err := os.Stat(table.StatusPath); os.IsNotExist(err) {
                err := os.MkdirAll(table.StatusPath, 0644)
                if err != nil {
                    util.Emit("Failed to create status path '%s'. Error was: %s\n", table.StatusPath, err)
                    return err
                }
            }
        }
    }
    return nil
}

func (sens *DBSensor) MergeConfig(to *DBConfig, from DBConfig) (err error) {
    to.Databases = append(to.Databases, from.Databases...)
    return nil
}

func getDecoder(result interface{}) (*mapstructure.Decoder, error) {
    return mapstructure.NewDecoder(&mapstructure.DecoderConfig{
        TagName:          "json",
        Result:           result,
        WeaklyTypedInput: false})
}

func (senc *DBSensor) LoadConfig(file_or_directory string) (err error) {
    config_files, err := util.DiscoverConfigs(file_or_directory)
    for _, filename := range config_files {
        additional_config, err := util.LoadConfig(filename)
        if err == nil {
            var result DBConfig
            decoder, err := getDecoder(&result)
            if err != nil {
                return err
            }
            err = decoder.Decode(additional_config)
            // err = util.DecodeStruct(&result, additional_config)
            if err == nil {
                err = senc.MergeConfig(&senc.DriverConfig, result)
            }
        }
        if err != nil {
            util.Fault("Could not load config file %s: %s", filename, err)
            return err
        }
    }
    return senc.FinalizeConfig(&senc.DriverConfig)
}

var s = DBSensor{}

func init() {
    sensor.Register("database", &s)
}
