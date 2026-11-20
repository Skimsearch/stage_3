$ErrorActionPreference = "Stop"
mvn clean package -DskipTests
if ($LASTEXITCODE -ne 0) {
    throw "Error en la compilación de Maven (código $LASTEXITCODE)"
}
Write-Host "Build completado: worker/target/worker.jar listo." -ForegroundColor Green