module AssignmentStrategyTests

open NUnit.Framework
open FSharp.Control
open Kafunk

[<Test>]
let ``ConsumerGroup.AssignmentStratgies.Range should assign all partitions and include all members when pc <= mc``() = 
  for pc in [1..256] do
  for mc in [1..pc] do

    let partitions : Partition[] = 
      Array.init pc id

    let members : MemberId[] = 
      Array.init mc (sprintf "%04i")

    let assignments = ConsumerGroup.AssignmentStratgies.RangeAssign partitions members

    let assignedPartitions = 
      assignments
      |> Seq.collect snd
      |> Seq.sort
      |> Seq.toList

    let membersWithAssignments =
      assignments
      |> Seq.choose (fun (m,ps) -> if ps.Length > 0 then Some m else None)
      |> Seq.sort
      |> Seq.toList

    shouldEqual (List.ofSeq partitions) (assignedPartitions) (Some (sprintf "partitions|pc=%i mc=%i" pc mc))
    shouldEqual (List.ofSeq members) (membersWithAssignments) (Some (sprintf "members|pc=%i mc=%i" pc mc))