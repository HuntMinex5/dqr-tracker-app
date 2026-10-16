@echo off
setlocal
cd /d "%~dp0"

echo ==================================================
echo   DQR Tracker - dong goi app Windows rieng
echo ==================================================
echo.

REM Tu tim index.js (giong build-exe-electron): thu trong CHINH thu muc nay truoc,
REM khong co thi thu o thu muc cha.
set "SRC=index.js"
if exist "index.js" (
  set "SRC=index.js"
) else if exist "..\index.js" (
  set "SRC=..\index.js"
) else (
  echo [LOI] Khong thay file index.js ^(da thu ca trong thu muc nay va thu muc cha^).
  echo Hay copy file index.js cua ban vao CHUNG thu muc voi build.bat nay.
  echo ^(Can file nay de app chay "local agent" ngam tren may user - lam Auto Rejoin /
  echo Performance / Config hoat dong that su. Thieu file nay app van mo duoc nhung 3
  echo chuc nang do se bao loi.^)
  pause
  exit /b 1
)
echo Dung index.js tai: %SRC%

if not exist "app" mkdir app
echo [1/3] Copy index.js vao app\index.js ^(KHONG lam roi/obfuscate - giu nguyen, da xac
echo nhan chay on dinh^)...
copy /y "%SRC%" "app\index.js" >nul
if errorlevel 1 (
  echo [LOI] Copy file that bai.
  pause
  exit /b 1
)

if not exist "node_modules" (
  echo [2/3] Cai Electron + cong cu build ^(chi lan dau, can internet^)...
  call npm install --no-fund --no-audit
  if errorlevel 1 (
    echo [LOI] npm install bi loi. Kiem tra lai da cai Node.js va co internet chua.
    pause
    exit /b 1
  )
) else (
  echo [2/3] Da cai truoc do, bo qua npm install.
)

echo [3/3] Dong goi thanh app Windows rieng...
REM Bo qua buoc tim/tai cong cu ky so (code signing) - khong can cho build ca nhan
REM khong ky so, va buoc nay hay loi "Cannot create symbolic link" tren Windows neu
REM chua bat Developer Mode / khong chay voi quyen Administrator.
set CSC_IDENTITY_AUTO_DISCOVERY=false
call npx electron-builder --win nsis
if errorlevel 1 (
  echo [LOI] Dong goi app bi loi. Xem thong bao loi phia tren.
  pause
  exit /b 1
)

echo.
echo Xong! File cai dat (Setup.exe) nam trong thu muc:  %cd%\dist\
echo Day la trinh CAI DAT that (nhu cac app khac): chay no se hoi cai vao dau,
echo tao shortcut Desktop + Start Menu, cai xong tu mo app - khong con la file
echo .exe roi chay truc tiep nua.
echo App se TU DONG mo thang trackerdqrtbao.online, khong can nhap gi ca.
echo Khi cai / mo app, Windows se hoi "Cho phep ung dung nay thay doi tren thiet bi?"
echo (UAC) - bam Yes. App can quyen nay de Auto Rejoin / Performance dieu khien duoc Roblox.
echo Gui file Setup.exe nay cho ai muon dung - ho chi can chay va bam Install la vao thang
echo tracker, dang nhap Discord nhu tren web, cua so rieng khong thanh dia chi.
echo ^(Neu sau nay doi domain, mo app len vao menu "Server" o tren ^> "Doi server
echo URL..." de sua.^)
echo.
pause
