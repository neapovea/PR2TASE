# Imagen base ligera de Java 17 basada en entorno Ubuntu
FROM eclipse-temurin:17-jdk-jammy

# Actualizamos repositorios, instalamos Git y limpiamos la caché temporal
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Directorio de trabajo dentro del contenedor
WORKDIR /app

COPY iniciar.sh /app/iniciar.sh

RUN chmod +x /app/iniciar.sh 


ENTRYPOINT ["sh", "/app/iniciar.sh"]

