/// Windlows pre-load dll from x86/x64 folder depending on Environment.Is64BitProcess
module internal Kafunk.Native.Loader 

open System
open System.Runtime.InteropServices
open System.IO

[<DllImport("Kernel32.dll")>]
extern IntPtr private LoadLibrary(string _path)

//
// Unix
//
let RTLD_NOW = 2
    
[<DllImport("libdl")>]
extern IntPtr private dlopen(string _fileName, int _flags)

/// Load assembly relative to executing assembly's CodeBase.
/// This function will not work for multi-assembly configuration, but is ok for kafunk for now.
/// More elaborative loading strategies can be found here:
/// https://github.com/mellinoe/nativelibraryloader
let private resolveLibPath name =
    System.Reflection.Assembly.GetExecutingAssembly().CodeBase
    |> fun path -> (new Uri(path)).LocalPath
    |> Path.GetDirectoryName
    |> fun path -> Path.Combine(path, name)

let private loadWin name =
    let path = resolveLibPath name 
    let ptr = LoadLibrary path

    if ptr = IntPtr.Zero then
        failwithf "Failed to load native dll '%s'" name

let load name = lazy(
    match (Environment.Is64BitProcess, Environment.OSVersion.Platform) with
        | (true, PlatformID.Win32NT) -> loadWin (sprintf "x64\\%s.dll" name)
        | (false, PlatformID.Win32NT) -> loadWin (sprintf "x86\\%s.dll" name)
        | _ -> ()
)


 