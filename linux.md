## Set up VNC on Ubuntu

* use 16.04, 18.04 can not offer vnc starting on boot!

#### Step 1: install vnc on ubuntu

```bash
sudo apt-get install x11vnc
```

#### Step 2: setup login password and save it on ubuntu

```bash
x11vnc -storepasswd
```
* By default the passwd is stored at `/home/jacob/.vnc/passwd`.
* By root rule, `sudo su`, it's stored at `/root/.vnc/passwd`.

#### Step 3: check the ip address of ubuntu

```bash
ip addr show
```

#### Step 4: start service

```bash
x11vnc -usepw
```
* the default port number is `5900`

#### Step 5: run vnc on boot

set it as systemctl serive and run it on boot.
```bash
sudo touch /lib/systemd/system/x11vnc.service
cat > /lib/systemd/system/x11vnc.service << EOF
[Unit]
Description=Start x11vnc at startup.
After=multi-user.target

[Service]
Type=simple
ExecStart=/usr/bin/x11vnc -auth guess -forever -loop -noxdamage -repeat -rfbauth /etc/x11vnc.pass -rfbport 5900 -shared

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl enable x11vnc.service
sudo systemctl daemon-reload
```
to check the status
```bash
sudo systemctl status x11vnc
```

#### All in One

1. create a bash file as below, named `vnc-setup.sh`
```bash
# Step 1 - Install X11VNC 
sudo apt-get install x11vnc -y

# Step 2 - Specify Password to be used for VNC Connection 
sudo x11vnc -storepasswd /etc/x11vnc.pass

# Step 3 - Create the Service Unit File
sudo touch /lib/systemd/system/x11vnc.service
cat > /lib/systemd/system/x11vnc.service << EOF
[Unit]
Description=Start x11vnc at startup.
After=multi-user.target

[Service]
Type=simple
ExecStart=/usr/bin/x11vnc -auth guess -forever -loop -noxdamage -repeat -rfbauth /etc/x11vnc.pass -rfbport 5900 -shared

[Install]
WantedBy=multi-user.target
EOF

# Step 4 -Configure the Service 
echo "Configure Services"
sudo systemctl enable x11vnc.service
sudo systemctl daemon-reload

sleep 10

# Step 5 - Restart System 
sudo shutdown -r now
```
2. give permission `sudo chmod +x vnc-setup.sh`
3. run it `sudo ./vnc-setup.sh`

---

## Run a Script on Boot

#### Step 1: create `rc.local`

```bash
sudo vi /etc/rc.local
```

#### Step 2: write commmands

```bash
#!/bin/sh -e
nohup /home/ubuntu/miniconda3/bin/jupyter notebook --allow-root --notebook-dir=/home/ubuntu
exit 0
```

#### Step 3: give permission 

```bash
sudo chmod +x /etc/rc.local
```

#### Step 4: test

```bash
sudo /etc/rc.local start
```