FROM flink:latest

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

RUN apt-get update -y && \
apt-get install -y git cmake build-essential gdb lcov pkg-config \
      libbz2-dev libffi-dev libgdbm-dev libgdbm-compat-dev liblzma-dev \
      libncurses5-dev libreadline6-dev libsqlite3-dev libssl-dev \
      lzma lzma-dev tk-dev uuid-dev zlib1g-dev && \
wget https://www.python.org/ftp/python/3.9.10/Python-3.9.10.tgz && \
tar -xvf Python-3.9.10.tgz && \
cd Python-3.9.10 && \
./configure --without-tests --enable-shared && \
make -j6 && \
make install && \
ldconfig /usr/local/lib && \
cd .. && rm -f Python-3.9.10.tgz && rm -rf Python-3.9.10 && \
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# install PyFlink

# COPY apache-flink*.tar.gz /
COPY jars/ /opt/flink/jars
# RUN python -m pip install apache-flink river==0.13.0
