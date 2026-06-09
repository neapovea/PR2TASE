#!/bin/bash

ls -ltr /app/PR2TASE
ls -ltr /app


echo "-*---------------------------------------Probando TEST." $OPCION_TEST
# 1. Comprobar si se ha pasado al menos un parámetro
if [ -z "$OPCION_TEST" ]; then
    echo "Error: No se ha proporcionado parámetro OPCION_TEST."
    echo "Uso: $0 <1|2|3|4>"
    exit 1
fi

if [ -z "$CANTIDAD_NODOS" ]; then
    echo "Error: No se ha proporcionado parámetro CANTIDAD_NODOS."
    echo "Uso: $0 <10|20|30|50>"
    exit 1
fi


git config --global init.defaultBranch main
git config --global --add safe.directory /app/PR2TASE


git clone https://github.com/neapovea/PR2TASE.git /app/PR2TASE

## cambiar a ruta de repo
cd PR2TASE

git branch 

echo "-*---------------------------------------Compilar."
# COMPILAR
find src -name "*.java" > sources.txt
javac -version
javac --release 17 -d bin -cp "lib/*" @sources.txt



# MOSTRAR RESULTAdos
echo "-*---------------------------------------Comprobar compilación."
echo "-*---------------------------------------Probando TEST." $OPCION_TEST
ls  -lh bin/recipes_service/ServerData.class bin/recipes_service/tsae/data_structures/* bin/recipes_service/tsae/sessions/*

chmod 777 results
cd scripts
chmod +x runN.sh
chmod 777 config.properties

# 3. Evaluar el parámetro y lanzar el comando correspondiente
echo "Opción $OPCION_TEST recibida. Lanzando comando para TEST $OPCION_TEST con  $CANTIDAD_NODOS  nodos"
date
case "$OPCION_TEST" in
    1) 
        echo "Test de Escala"
        ;;
    2) 
        echo "Test de Alta Dinamicidad (Red Inestable)"
        sed -i 's/^probDisconnect=.*/probDisconnect=0.2/' config.properties
        sed -i 's/^probReconnect=.*/probReconnect=0.1/' config.properties
        ;;
    3) 
        echo "Test de Diseminación Inmediata (Rumor-mongering)"
        sed -i 's/^propDegree=.*/propDegree=2/' config.properties
        ;;
    4) 
        echo "Test de Red Agresiva"
        sed -i 's/^sessionPeriod=.*/sessionPeriod=5/' config.properties
        ;;
    5) 
        echo "Test de Red Pacífica"
        sed -i 's/^sessionPeriod=.*/sessionPeriod=40/' config.properties
        ;;        
    6)  
        echo "El Salto a Distribuido"
        sed -i 's/^executionMode=.*/executionMode=remoteMode/' config.properties
        ;;

    7) 
        echo "Carga de activadad"
        sed -i 's/^probCreate=.*/probCreate=0.75/' config.properties
        sed -i 's/^probDel=.*/probDel=0.5/' config.properties
        ;;

    *)
        # Aquí nunca llega
        echo "Error: Parámetro no válido ('$OPCION_TEST')."
        echo "Los valores permitidos son únicamente 2, 3 o 4."
        exit 1
        ;;
esac

./runN.sh 10 20004 $CANTIDAD_NODOS --logResults -path ../results 2>&1 | tee resultados.run$CANTIDAD_NODOS.txt

cd ..

echo "-*---------------------------------------Comprobar resultados."
date
ls  -lh bin/recipes_service/ServerData.class bin/recipes_service/tsae/data_structures/* bin/recipes_service/tsae/sessions/*  scripts/Results*; cat scripts/Results 

echo "-*---------------------------------------Exportando datos al PC."

# crear estructura de directorios
mkdir /app/salida_host/lsimLogs$OPCION_TEST/
mkdir /app/salida_host/results$OPCION_TEST/
mkdir /app/salida_host/scripts$OPCION_TEST/

# Copiar el directorio bin completo
cp -r lsimLogs /app/salida_host/lsimLogs$OPCION_TEST/
# Copiar los logs de resultados de los scripts
cp -r results /app/salida_host/results$OPCION_TEST/
cp -r scripts /app/salida_host/scripts$OPCION_TEST/


echo "-*---------------------------------------Ejecución finalizada."

