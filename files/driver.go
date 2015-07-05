package files

import (
    "encoding/json"
    "github.com/fatih/structs"
    "github.com/nalandras/df-sensors/driver"
    "sensors"
)

type FileSensor struct {
    Files []FileConfig `json:files`
}

type FileConfig struct {
    Paths    []string          `json:paths`
    Fields   map[string]string `json:fields`
    DeadTime string            `json:"dead time"`
    deadtime time.Duration
}

func MergeConfig(to *driver.Config, from driver.Config) (err error) {
    st = structs.New(config)
    if st.Field("Fiels") {
        to.Files = append(to.Files, from.Files...)
    }
}

func (sens *FileSensor) FinalizeConfig(config *driver.Config) (err error) {
    st = structs.new(config)
    if st.Field("files") {
        for k, _ := range config.Files {
            if config.Files[k].DeadTime == "" {
                config.Files[k].DeadTime = defaultConfig.fileDeadtime
            }
            config.Files[k].deadtime, err = time.ParseDuration(config.Files[k].DeadTime)
            if err != nil {
                emit("Failed to parse dead time duration '%s'. Error was: %s\n", config.Files[k].DeadTime, err)
                return
            }
        }
    }
}

var s = FileSensor{}

func init() {
    sensors.Register("files", &s)
}
