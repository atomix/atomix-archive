FROM anapsix/alpine-java:8_server-jre

ARG VERSION
RUN mkdir -p /root/atomix
COPY target/atomix.tar.gz /root/atomix/atomix.tar.gz
RUN tar -xvf /root/atomix/atomix.tar.gz -C /root/atomix && rm /root/atomix/atomix.tar.gz

WORKDIR /root/atomix

EXPOSE 5678

ENTRYPOINT ["./bin/atomix-server"]
