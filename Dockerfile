FROM gradle:9.9.1-jdk21 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts gradle.properties ./
COPY app app
RUN gradle clean installDist --no-daemon
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/build/install/trama/ /app/
EXPOSE 8080
ENV JAVA_OPTS=""
CMD ["sh", "-c", "bin/trama"]
