namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("kafunk")>]
[<assembly: AssemblyProductAttribute("kafunk")>]
[<assembly: AssemblyDescriptionAttribute("F# client for Kafka")>]
[<assembly: AssemblyVersionAttribute("0.0.28")>]
[<assembly: AssemblyFileVersionAttribute("0.0.28")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.28"
    let [<Literal>] InformationalVersion = "0.0.28"
