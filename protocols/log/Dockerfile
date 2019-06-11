FROM anapsix/alpine-java:8_server-jre

ARG VERSION
RUN mkdir -p /root/atomix
COPY target/atomix-server-log.tar.gz /root/atomix/atomix-server-log.tar.gz
RUN tar -xvf /root/atomix/atomix-server-log.tar.gz -C /root/atomix && \
    chmod u+x /root/atomix/bin/atomix-server && \
    rm /root/atomix/atomix-server-log.tar.gz

WORKDIR /root/atomix

EXPOSE 5678

ENTRYPOINT ["./bin/atomix-server"]
