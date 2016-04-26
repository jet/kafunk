namespace KafkaFs

open System
open KafkaFs.Prelude
open KafkaFs.Configuration

module Cluster =

  type Cluster = {
    clientId : DVar<string>
    brokers : DVar<Uri []>
  }

