# ---------- Etapa 1: compilar y ejecutar las pruebas ----------
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app

# Primero solo lo necesario para descargar dependencias:
# si el código cambia pero el pom no, Docker reutiliza esta capa.
COPY mvnw pom.xml ./
COPY .mvn .mvn
RUN sed -i 's/\r$//' mvnw && chmod +x mvnw
RUN ./mvnw -B -q dependency:go-offline || true

# Luego el código. "package" ejecuta las pruebas: si fallan, la imagen no se construye.
COPY src src
RUN ./mvnw -B package

# ---------- Etapa 2: imagen final, solo con el JRE ----------
FROM eclipse-temurin:21-jre
WORKDIR /app

# La aplicación no corre como root dentro del contenedor
RUN useradd --system --uid 1001 spring
COPY --from=build /app/target/*.jar app.jar
USER spring

EXPOSE 8086
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
