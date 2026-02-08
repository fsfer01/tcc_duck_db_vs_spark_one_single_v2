FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3.8 \
    python3.8-venv \
    python3-pip \
    curl \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Definir JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Criar virtualenv no volume /opt/venv
RUN python3.8 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Atualizar pip (não instala libs ainda)
RUN pip install --upgrade pip

# Configurar SPARK_HOME
ENV SPARK_HOME=/opt/venv/lib/python3.8/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH

# Diretório de trabalho
WORKDIR /app

# Entrar em shell por padrão
CMD ["bash"]
