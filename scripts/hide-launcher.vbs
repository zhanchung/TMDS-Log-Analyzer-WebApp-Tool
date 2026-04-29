' Hidden launcher: runs the file passed as arg 1 with no visible window.
' Used by TMDS-Server-Switch.bat to keep the server running with no console window.
' Supports .ps1, .bat/.cmd, or any other command-line target.
Option Explicit

Dim shell, target, lower, command
Set shell = CreateObject("WScript.Shell")

If WScript.Arguments.Count < 1 Then
  WScript.Quit 1
End If

target = WScript.Arguments(0)
lower = LCase(target)

If Right(lower, 4) = ".ps1" Then
  command = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File """ & target & """"
ElseIf Right(lower, 4) = ".bat" Or Right(lower, 4) = ".cmd" Then
  command = "cmd.exe /c """ & target & """"
Else
  command = target
End If

shell.Run command, 0, False

Set shell = Nothing
