namespace Kafunk.Native

module Loader = 
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

    /// Load assembly relative to executing assembly's CodeBase plus "lib\win64" and "lib\win32" folders.
    /// This function will not work for multi-assembly configuration, but is ok for kafunk for now.
    /// More elaborative loading strategies can be found here:
    /// https://github.com/mellinoe/nativelibraryloader
    let private loadWin name =
        let ptr = 
                System.Reflection.Assembly.GetExecutingAssembly().CodeBase
                |> fun path -> (new Uri(path)).LocalPath
                |> Path.GetDirectoryName
                |> fun path -> Path.Combine(path, name)
                |> LoadLibrary

        if ptr = IntPtr.Zero then
            failwithf "Failed to load native dll '%s'" name

    let private loadUnix name: unit =
        let ptr = dlopen(name, RTLD_NOW)
        if ptr = IntPtr.Zero then
            failwith (sprintf "Failed to load dynamic library '%s'" name)

    let load name = lazy(
        match (Environment.Is64BitProcess, Environment.OSVersion.Platform) with
            | (true, PlatformID.Win32NT) -> loadWin (sprintf "lib\\win64\\%s" name)
            | (false, PlatformID.Win32NT) -> loadWin (sprintf "lib\\win32\\%s" name)
            | (_, PlatformID.Unix) -> loadUnix name
            | _ -> failwithf "Unsupported platform for LZ4 compression: %O, 64 bits: %O" Environment.OSVersion.Platform Environment.Is64BitProcess
    )


 