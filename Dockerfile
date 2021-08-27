FROM golang:1.17

RUN apt-get update && apt-get -y install tini

RUN git clone https://github.com/msk-mind/minfs.git

RUN cd minfs && go get -d -v

RUN  cd minfs && go install -v

CMD ["minfs"]