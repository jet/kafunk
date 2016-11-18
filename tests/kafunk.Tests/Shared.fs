namespace Kafunk

open NUnit.Framework

[<AutoOpen>]
module Testing =

  let shouldEqual (expected:'a) (actual:'a) (msg:string option) =
    if expected <> actual then
      let msg = 
        match msg with
        | Some msg -> sprintf "expected=%A\nactual=%A message=%s" expected actual msg
        | None -> sprintf "expected=%A\nactual=%A" expected actual
      Assert.Fail msg