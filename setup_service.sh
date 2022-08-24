##########################################################

yum install -y fuse

cat << EOF > /etc/fuse.conf
user_allow_other
EOF

wget --no-check-certificate -O /usr/bin/mskvfs https://raw.githubusercontent.com/msk-mind/minfs/v0.1.0/mskvfs
chmod +x /usr/bin/mskvfs

groupadd -r mskvfs
useradd -r -g mskvfs -d /var/lib/mskvfs -s /sbin/nologin mskvfs
mkdir -p /var/lib/mskvfs
chown -R mskvfs:mskvfs /var/lib/mskvfs

mkdir -p /mskvfs/localhost
chown mskvfs:mskvfs /mskvfs/localhost

mkdir /run/mskvfs
chown -R mskvfs:mskvfs /run/mskvfs

touch /var/log/minfs.log
chown mskvfs:mskvfs /var/log/minfs.log


cat << EOF > /lib/systemd/system/mskvfs.service
[Unit]
Description="MSK Virtual Filesystem"

[Service]
Type=simple
WorkingDirectory=/var/lib/mskvfs
User=mskvfs
Group=mskvfs
EnvironmentFile=/etc/default/mskvfs
ExecStart=/usr/bin/mskvfs -o cache=/run/mskvfs/db,quota=0 /mskvfs/localhost https://localhost:9000
Restart=always
LimitNOFILE=65536
TimeoutStopSec=infinity
SendSIGKILL=no
EOF

cat << EOF > /etc/default/mskvfs
MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY
MINIO_SECRET_KEY=$MINIO_SECRET_KEY
EOF
chown mskvfs:mskvfs /etc/default/mskvfs

systemctl daemon-reload
systemctl restart mskvfs
systemctl status mskvfs

sleep 5
ls /mskvfs/*/*

##########################################################
