$ErrorActionPreference = "Stop"

$BUILD_MODE = $Env:BUILD_MODE
$VS_PLATFORM = $Env:VS_PLATFORM

if(!$BUILD_MODE) {$BUILD_MODE = "Debug"}
if(!$VS_PLATFORM) {$VS_PLATFORM = "Win32"}

cd build
echo "ctest.exe -C $BUILD_MODE ."
ctest.exe -C $BUILD_MODE .
