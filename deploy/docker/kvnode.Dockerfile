FROM alpine:3.20
RUN apk add --no-cache iptables tini
COPY bin/kvnode-linux /usr/local/bin/kvnode
COPY deploy/docker/supervise.sh /usr/local/bin/supervise.sh
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/supervise.sh"]
