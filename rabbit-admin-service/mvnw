#!/bin/sh
set -eu

# DSY1107 - Maven Wrapper para Linux/macOS.
# No requiere Maven instalado globalmente. La primera ejecucion descarga Maven 3.9.11.

PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROPS="$PROJECT_DIR/.mvn/wrapper/maven-wrapper.properties"

if [ ! -f "$PROPS" ]; then
  echo "ERROR: No se encontro $PROPS" >&2
  exit 1
fi

MAVEN_VERSION=$(sed -n 's/^mavenVersion=//p' "$PROPS" | tr -d '\r' | tail -n 1)
DISTRIBUTION_URL=$(sed -n 's/^distributionUrl=//p' "$PROPS" | tr -d '\r' | tail -n 1)

if [ -z "$MAVEN_VERSION" ] || [ -z "$DISTRIBUTION_URL" ]; then
  echo "ERROR: maven-wrapper.properties debe contener distributionUrl y mavenVersion." >&2
  exit 1
fi

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: No se encontro Java. Instale/configure JDK 21 y verifique con: java -version" >&2
  exit 1
fi

CACHE_ROOT="$PROJECT_DIR/.mvn/wrapper/dists"
DIST_DIR="$CACHE_ROOT/apache-maven-$MAVEN_VERSION-bin"
MAVEN_HOME="$DIST_DIR/apache-maven-$MAVEN_VERSION"
ZIP_PATH="$DIST_DIR/apache-maven-$MAVEN_VERSION-bin.zip"

if [ ! -x "$MAVEN_HOME/bin/mvn" ]; then
  mkdir -p "$DIST_DIR"
  echo "Maven Wrapper DSY1107: Maven $MAVEN_VERSION no esta en cache."
  echo "Descargando Maven. La primera ejecucion requiere conexion a Internet..."

  if command -v curl >/dev/null 2>&1; then
    curl -fL "$DISTRIBUTION_URL" -o "$ZIP_PATH"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$ZIP_PATH" "$DISTRIBUTION_URL"
  else
    echo "ERROR: se requiere curl o wget para descargar Maven." >&2
    exit 1
  fi

  if ! command -v unzip >/dev/null 2>&1; then
    echo "ERROR: se requiere unzip para instalar Maven automaticamente." >&2
    exit 1
  fi

  echo "Descomprimiendo Maven..."
  unzip -q -o "$ZIP_PATH" -d "$DIST_DIR"
  rm -f "$ZIP_PATH"
fi

exec "$MAVEN_HOME/bin/mvn" "$@"
