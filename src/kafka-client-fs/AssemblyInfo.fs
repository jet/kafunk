namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("kafka-client-fs")>]
[<assembly: AssemblyProductAttribute("kafka-client-fs")>]
[<assembly: AssemblyDescriptionAttribute("F# client for Kafka")>]
[<assembly: AssemblyVersionAttribute("1.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0"
