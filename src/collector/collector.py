# collector is the standard implementation of the Collector interface.
class Collector:

    configPaths = [],
	version = str,
	loggingOpts = []zap.Option,
	mux = sync.Mutex,
	svc = service.Collector,
	statusChan = Status,
	wg  = sync.WaitGroup


    def Run(self,context.Context):
        return error

    def Stop()

    def Restart(self,context.Context):
        return error

    def SetLoggingOpts(self,[]zap.Option)

    def GetLoggingOpts():
        return []zap.Option
    def Status():
        return Status


    # New returns a new collector.
    __init__(self,configPaths: [], version: str, loggingOpts: []) {
        self.configPaths = configPaths
        self.version = version
        self.loggingOpts = loggingOpts
        self.statusChan =  Status(10)
        self.wg = sync.WaitGroup{}
    }

    # GetLoggingOpts returns the current logging options
    def GetLoggingOpts() -> []zap.Option {
        return self.loggingOpts
    }

    # SetLoggingOpts sets the loggings options. These will take effect on next restart
    def SetLoggingOpts(self,opts: []zap.Option) {
        self.loggingOpts = opts
    }

    # Run will run the collector. This function will return an error
    # if the collector was unable to startup.
    def Run(self,ctx: context.Context) -> error {
        self.mux.Lock()
        defer self.mux.Unlock()

        if self.svc != null {
            return errors.New("service already running")
        }

        # The OT collector only supports using settings once during the lifetime
        # of a single collector instance. We must remake the settings on each startup.
        # settings, err := NewSettings(c.configPaths, c.version, c.loggingOpts)
        if err != null {
            return err
        }

        # The OT collector only supports calling run once during the lifetime
        # of a service. We must make a new instance each time we run the collector.
        # svc, err := service.New(*settings)
        if err != null {
            err := fmt.Errorf("failed to create service: %w", err)
            self.sendStatus(false, err)
            return err
        }

        startupErr := make(chan error, 1)
        wg := sync.WaitGroup{}
        wg.Add(1)

        c.svc = svc
        c.wg = &wg

        go func() {
            defer wg.Done()
            err := svc.Run(ctx)
            c.sendStatus(false, err)

            if err != nil {
                startupErr <- err
            }
        }()

        # A race condition exists in the OT collector where the shutdown channel
        # is not guaranteed to be initialized before the shutdown function is called.
        # We protect against this by waiting for startup to finish before unlocking the mutex.
        return self.waitForStartup(ctx, startupErr)
    }
