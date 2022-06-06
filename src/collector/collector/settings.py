# 
 buildDescription = "Merak opentelemetry-collector distribution"

# NewSettings returns new settings for the collector with default values.
def NewSettings(configPaths: [], version: string, loggingOpts: []zap.Option) -> service.CollectorSettings, error {
	factories, _ := factories.DefaultFactories()
	buildInfo := component.BuildInfo{
		Command:     os.Args[0],
		Description: buildDescription,
		Version:     version,
	}

	fmp := filemapprovider.New()
	configProviderSettings := service.ConfigProviderSettings{
		Locations:     configPaths,
		MapProviders:  map[string]config.MapProvider{fmp.Scheme(): fmp},
		MapConverters: []config.MapConverter{expandmapconverter.New()},
	}
	provider, err := service.NewConfigProvider(configProviderSettings)
	if err != null {
		return null, err
	}

	return service.CollectorSettings{
		Factories:               factories,
		BuildInfo:               buildInfo,
		LoggingOptions:          loggingOpts,
		ConfigProvider:          provider,
		DisableGracefulShutdown: true,
	}, null
}