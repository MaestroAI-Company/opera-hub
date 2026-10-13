; per user install, no admin rights, starts at login
Unicode true
!include "MUI2.nsh"

!ifndef VERSION
  !define VERSION "0.0.0"
!endif
!ifndef DIST
  !define DIST "..\..\dist"
!endif
!define UNINSTALL_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\OperaHub"

Name "Opera Hub"
OutFile "${DIST}\opera-hub-setup-windows-x86_64.exe"
InstallDir "$LOCALAPPDATA\Programs\Opera Hub"
RequestExecutionLevel user
SetCompressor /SOLID lzma
VIProductVersion "${VERSION}.0"
VIAddVersionKey "ProductName" "Opera Hub"
VIAddVersionKey "CompanyName" "Maestro AI"
VIAddVersionKey "ProductVersion" "${VERSION}"
VIAddVersionKey "FileDescription" "Opera Hub installer"
VIAddVersionKey "FileVersion" "${VERSION}"

!define MUI_FINISHPAGE_RUN "$INSTDIR\opera-hubw.exe"
!define MUI_FINISHPAGE_RUN_TEXT "Open Opera Hub"
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_LANGUAGE "English"
!insertmacro MUI_LANGUAGE "French"

Section "Opera Hub"
  ; a running hub locks its files
  nsExec::Exec 'taskkill /IM opera-hubw.exe /F'
  Sleep 1000
  SetOutPath "$INSTDIR"
  File "${DIST}\opera-hub.exe"
  File "${DIST}\opera-hubw.exe"
  WriteUninstaller "$INSTDIR\uninstall.exe"
  CreateShortCut "$SMPROGRAMS\Opera Hub.lnk" "$INSTDIR\opera-hubw.exe"

  ; login start, and the hub runs now
  nsExec::Exec '"$INSTDIR\opera-hub.exe" service install'

  WriteRegStr HKCU "${UNINSTALL_KEY}" "DisplayName" "Opera Hub"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "DisplayVersion" "${VERSION}"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "Publisher" "Maestro AI"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "DisplayIcon" "$INSTDIR\opera-hubw.exe"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "InstallLocation" "$INSTDIR"
  WriteRegStr HKCU "${UNINSTALL_KEY}" "UninstallString" '"$INSTDIR\uninstall.exe"'
  WriteRegDWORD HKCU "${UNINSTALL_KEY}" "NoModify" 1
  WriteRegDWORD HKCU "${UNINSTALL_KEY}" "NoRepair" 1
SectionEnd

; hub data and pairings stay, like any app data
Section "Uninstall"
  nsExec::Exec '"$INSTDIR\opera-hub.exe" service uninstall'
  Sleep 1000
  Delete "$INSTDIR\opera-hub.exe"
  Delete "$INSTDIR\opera-hubw.exe"
  Delete "$INSTDIR\uninstall.exe"
  RMDir "$INSTDIR"
  Delete "$SMPROGRAMS\Opera Hub.lnk"
  DeleteRegKey HKCU "${UNINSTALL_KEY}"
SectionEnd
