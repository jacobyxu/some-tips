## Export Excel to PDF

```vb
Public Function listFilesDir(ByVal sPath As String) As String()

    Dim sFile As String
    Dim fileList() As String
    Dim fileicount As Integer

    If Right(sPath, 1) <> "\" Then
        sPath = sPath & "\"
    End If

    sFilter = "*.*"
    'call with path "initializes" the dir function and returns the first file name
    sFile = Dir(sPath & sFilter)

   'call it again until there are no more files
    Do Until sFile = ""
   
        ReDim Preserve fileList(fileicount)
        fileList(fileicount) = sFile
        fileicount = fileicount + 1

        'subsequent calls without param return next file name
        sFile = Dir

    Loop
   
    listFilesDir = fileList

End Function

Function isInArray(stringToBeFound As String, arr As Variant) As Boolean

  isInArray = (UBound(Filter(arr, stringToBeFound)) > -1)
 
End Function

Function savePdf(wbDir As Variant, pdfDir As Variant, wbFile As Variant, tabList As Variant, _
                 Optional surfix As String = "xlsm", Optional printArea As String = "")
                 
    Dim wbSheets() As String
    Dim pdfFile As String
    Dim sh As Worksheet
    Dim icount As Integer
    Dim wb As Workbook

    icount = 0
    If LCase(Right(wbFile, 5)) = "." & surfix Then
       
        'save pdf name and dir
        pdfFile = pdfDir & "\" & Split(wbFile, ".")(0) & ".pdf"
       
        'open xlsx file
        Set wb = Application.Workbooks.Open(wbDir & "\" & wbFile)
       
        'save sheet names to an Array
        For Each sh In wb.Worksheets
            ifInArray = isInArray(sh.Name, tabList)
            If sh.Visible = xlSheetVisible And ifInArray Then
                ReDim Preserve wbSheets(icount)
                wbSheets(icount) = sh.Name
                'set print area
                If printArea <> "" Then
                    sh.PageSetup.printArea = printArea
                End If
                icount = icount + 1
            End If
        Next sh
       
        'no charts found
        If icount = 0 Then
            MsgBox "A PDF cannot be created because no sheets were found in " & wbFile, , "No Sheets Found"
            Exit Function
        End If
       
        'select saved sheet names
        wb.Sheets(wbSheets).Select
       
        'export pdf
        ActiveSheet.ExportAsFixedFormat _
            Type:=xlTypePDF, _
            Filename:=pdfFile, _
            Quality:=xlQualityStandard, _
            IncludeDocProperties:=True, _
            IgnorePrintAreas:=False, _
            OpenAfterPublish:=False

        wb.Close
       
    End If
End Function
Sub PrintAllSheetsToPDF()

Dim wbDir As String
Dim wbFiles() As String
Dim surfix As String
Dim printArea As String

'input
'wbDir = "C:\testout"
'pdfDir = "C:\testout"
'surfix = "xlsx"
'tabList = Array("Sheet1", "Sheet2")
'printArea = "$A$1:$F$10"

wbDir = Range("A2").Value
pdfDir = Range("B2").Value
surfix = Range("C2").Value
printArea = Range("D2").Value
tabList = Application.Transpose(Range("tabs").Value2)


'get list of files
wbFiles = listFilesDir(wbDir)

For Each wbFile In wbFiles
    x = savePdf(wbDir, pdfDir, wbFile, tabList, surfix, printArea)
Next wbFile

MsgBox "All " & surfx & " files have been converted to pdf files."

End Sub
```