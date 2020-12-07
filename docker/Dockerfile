#基础镜像
FROM centos:centos7

RUN yum update -y && \
    yum install -y python3-pip python3-dev

COPY ./requirements.txt /requirements.txt

RUN mkdir ~/.pip

COPY ./pip.conf ~/.pip/

WORKDIR /

RUN pip3 install -r requirements.txt

COPY . /

ENTRYPOINT [ "python3" ]

CMD [ "sampling_app/entrance.py" ]