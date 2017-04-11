namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("kafunk")>]
[<assembly: AssemblyProductAttribute("kafunk")>]
[<assembly: AssemblyDescriptionAttribute("F# client for Kafka")>]
[<assembly: AssemblyVersionAttribute("0.0.39")>]
[<assembly: AssemblyFileVersionAttribute("0.0.39")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.39"
    let [<Literal>] InformationalVersion = "0.0.39"
