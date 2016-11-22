module PreludeTests

open NUnit.Framework
open Kafunk
open System

[<Test>]
let ``should parse various uri formats`` () =
  
  let uris =
    [
      "localhost"
      "localhost:9092"
      "kafka://localhost/"
      "kafka://localhost:9092/"
      "tcp://localhost/"
      "tcp://localhost:9092/"
      "tcp://127.0.0.1:9092/"
    ]

  let actual = uris |> List.map KafkaUri.parse

  let expected = 
    [
      Uri("kafka://localhost:9092")
      Uri("kafka://localhost:9092")
      Uri("kafka://localhost:9092")
      Uri("kafka://localhost:9092")
      Uri("kafka://localhost:9092")
      Uri("kafka://localhost:9092")
      Uri("kafka://127.0.0.1:9092")
    ]

  shouldEqual expected actual None