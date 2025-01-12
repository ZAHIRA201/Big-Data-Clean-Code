FROM hjben/hbase:1.6.0-hadoop3.3.0

# ADD YUM MIRRORS AND UPDATE
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && \
    sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* && \
    yum update -y && \
    yum install -y python36-3.6.8 python3-pip-9.0.3
RUN pip3 install --upgrade pip && pip3 install numpy pandas scikit-build fuzzywuzzy[speedup]
