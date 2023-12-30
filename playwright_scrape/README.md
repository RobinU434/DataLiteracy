## Info

This subproject abuses all kinds of tools to get the job done, so dont expect something proper.

Concretely we learned that these websites are not as easy to scrape as one did hope, amongst others they rely on JS code that won't run when using stuff such as curl or wget.

As such we abuse a testig toolkit ([playwright](https://playwright.dev/python/)) to execute the websites, and click the cookie button (if required) and then safe the html files after execution in that testing toolkits more or less real browser (so hopefully after the JS executed).

## Installation

This was tested only on arch based distros, with podman(amd64) and docker(arm64).
Since none of these distros had a crontab by default (and using a crontab internal to the containers may not be a good idea if we want correct timing) I am using systemd-timers, which is kind of verbose, but hey, it provides me free logging.

So your distro needs either `docker` or `podman` **with a docker alias**, and then it needs to have `systemd-timers` (which should be included in all normal systemd installs), and in particular it should have a `timers` systemd target (and you may have to reboot after installation), otherwise the timer will not work.
Also, of course, your device needs to be on and online at the given time.

Then run `docker build -f Containerfile .. -t playwright_scrape` in this directory, ensure the directory referenced in the [service file](./playwright_scrape.service) is present (or change it accordingly), then add the service and timer file to the right directory (for arch based distros this was `/etc/systemd/system/`):

```sh
sudo sh -c '
    export SYSD_DIR=/etc/systemd/system/
    cp playwright_scrape.* $SYSD_DIR
'
```

Then reload the daemon (`systemctl daemon-reload`) and test that the service runs correctly (`sudo systemctl start playwright_scrape.service` + look at logs).

Then enable the timer (`sudo systemctl enable playwright_scrape.timer`), then **reboot** and check the timer is set correctly (`sudo systemctl list-timers --all`).
