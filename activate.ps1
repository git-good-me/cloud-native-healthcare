venv\Scripts\activate
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
$env:Path = $env:Path + ";C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot\bin"
Write-Host "Environment ready" -ForegroundColor Green