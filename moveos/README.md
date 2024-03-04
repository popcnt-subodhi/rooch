# MoveOS

MoveOS is a self-contained Move runtime environment, innovatively built upon
the [MoveVM](https://github.com/move-language/move)
by the [Rooch Network](https://rooch.network).

## Key Features

MoveOS enhances the MoveVM with a suite of functionalities designed to seamlessly integrate applications with the Move
runtime ecosystem:

1. State Management: Out-of-the-box, it offers a local database for state storage and retrieval, while also allowing
   applications to tailor this storage solution to their specific needs.
2. Rust-to-Move Extension Point ABI: Applications can harness Move to construct core logic and invoke it within Rust,
   facilitating effortless maintenance and progressive updates. This includes, but is not limited to, transaction
   verification logic (Account Abstraction).
3. State Proof: It furnishes state verifiability via a state tree construct.
4. Fraud Proof: It introduces an interactive fraud detection mechanism,
   leveraging [flexEmu](https://github.com/rooch-network/flexemu), indispensable for
   modular applications. In the forthcoming future, zero-knowledge proofs
   using [zkMove](https://github.com/young-rocks/zkmove) will be amalgamated.

## Usage

1. As a Rust library, it can be embedded within a blockchain or alternative applications.
2. As an autonomous entity, it can be accessed through a REST API or IPC (TODO).

## Guidance for Developers

Ensure that the crates within the `moveos` directory are independent of those outside its scope, preserving the
autonomous nature of the `moveos` crate.