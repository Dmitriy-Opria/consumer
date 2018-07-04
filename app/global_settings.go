package app

type globalSettings struct {
	DbUseMasterReading int    `key:"use_master_reading" default:"0"`
	CrawlerUserAgents  string `key:"crawler_user_agents" default:""`
}

func (app *App) initGlobalSettings() {
	settings := globalSettings{}
	app.mysqlClient.LoadGlobalSettings(&settings)
	app.gSettings = &settings
}

func (app *App) globalSettings() globalSettings {
	return *app.gSettings
}
