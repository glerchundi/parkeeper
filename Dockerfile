FROM       scratch
MAINTAINER Gorka Lerchundi Osa <glertxundi@gmail.com>
ADD        https://github.com/glerchundi/parkeeper/releases/download/v0.1.0/parkeeper-0.1.0-linux-amd64 /parkeeper
ENTRYPOINT ["/parkeeper"]
