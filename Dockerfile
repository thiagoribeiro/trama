FROM gradle:8.10.2-jdk21 AS build
WORKDIR /app
COPY gradle gradle
COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY build.gradle.kts settings.gradle.kts gradle.properties ./
COPY src src
RUN ./gradlew clean installDist

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/build/install/trama/ /app/
EXPOSE 8080
ENV JAVA_OPTS=""
CMD ["sh", "-c", "bin/trama"]
