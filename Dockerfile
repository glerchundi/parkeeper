FROM       scratch
MAINTAINER Gorka Lerchundi Osa <glertxundi@gmail.com>
ADD        parkeeper /parkeeper
ENTRYPOINT ["/parkeeper"]