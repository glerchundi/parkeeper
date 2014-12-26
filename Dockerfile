FROM       busybox
MAINTAINER Gorka Lerchundi Osa <glertxundi@gmail.com>
ADD        https://github.com/glerchundi/parkeeper/releases/download/v0.2.0/parkeeper-0.2.0-linux-amd64 /parkeeper
RUN        chmod +x /parkeeper
ENTRYPOINT ["/parkeeper"]
