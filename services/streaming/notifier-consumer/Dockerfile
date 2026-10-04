FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build

COPY pom.xml .
RUN mvn -q -B dependency:go-offline

COPY src ./src
RUN mvn -q -B package

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /build/target/notifier-consumer.jar app.jar

EXPOSE 8000

CMD ["java", "-jar", "app.jar"]
