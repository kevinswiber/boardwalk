# Changelog

All notable changes to this project will be documented in this file. See
[conventional commits](https://www.conventionalcommits.org/) for commit
guidelines.

- - -
## [v1.0.0](https://github.com/kevinswiber/boardwalk/compare/49873f07aa903cf7acd25f1e437b0fc556f75c5d..v1.0.0) - 2026-05-20
#### Features
- add graceful prebound listener serving - ([ba0712b](https://github.com/kevinswiber/boardwalk/commit/ba0712be5ff817a27b8a0de7fddf742f9d64edea)) - [@kevinswiber](https://github.com/kevinswiber)
- move job runner onto reusable runtime - ([126f116](https://github.com/kevinswiber/boardwalk/commit/126f1165fb26d3cafcf65acb641a37c76ea34329)) - [@kevinswiber](https://github.com/kevinswiber)
- move peer streams onto actor runtime - ([a8525ea](https://github.com/kevinswiber/boardwalk/commit/a8525ea4265ec19d8a2204543fa733e42b2f57e7)) - [@kevinswiber](https://github.com/kevinswiber)
- build Boardwalk from actor runtime - ([b957d9e](https://github.com/kevinswiber/boardwalk/commit/b957d9ef4e47e069c069a62cb6cb0ec908e97f73)) - [@kevinswiber](https://github.com/kevinswiber)
- add actor-backed HTTP core - ([db3cb12](https://github.com/kevinswiber/boardwalk/commit/db3cb12a134cd6e83234ebcc77d3629985f8b004)) - [@kevinswiber](https://github.com/kevinswiber)
- add job runner example - ([7a30649](https://github.com/kevinswiber/boardwalk/commit/7a306498829d010329e486ceb241265e1a524d7c)) - [@kevinswiber](https://github.com/kevinswiber)
- add #[actor] proc-macro for actor dispatch - ([03c0773](https://github.com/kevinswiber/boardwalk/commit/03c077314c5c5f8918d2db2277983a5979c58cb6)) - [@kevinswiber](https://github.com/kevinswiber)
- add deterministic job runner exemplar - ([edc666b](https://github.com/kevinswiber/boardwalk/commit/edc666b74e292fab1c5ce039eace1377bd9e3830)) - [@kevinswiber](https://github.com/kevinswiber)
- switch HTTP hypermedia to resource routes - ([91aa1a6](https://github.com/kevinswiber/boardwalk/commit/91aa1a6b45f4fd527cacc1ad14fd1925399acdbd)) - [@kevinswiber](https://github.com/kevinswiber)
- implement OverflowPolicy::Coalesce with per-subscription sidecar queue - ([e0e5b2f](https://github.com/kevinswiber/boardwalk/commit/e0e5b2f45f58a1fb61c733cf367404d6a4c720b4)) - [@kevinswiber](https://github.com/kevinswiber)
- publish state-change envelopes through context with causation populated - ([21b87ad](https://github.com/kevinswiber/boardwalk/commit/21b87ad2dfa0cfc414dfb4e8b7fed02fbb05fd19)) - [@kevinswiber](https://github.com/kevinswiber)
- add async EventBus::publish honoring Lossy+Backpressure capacity - ([0adee9f](https://github.com/kevinswiber/boardwalk/commit/0adee9f90e5877ca664585342887a94e09856237)) - [@kevinswiber](https://github.com/kevinswiber)
- node runtime with resource directory, actor execution, and app handle - ([87d1ae0](https://github.com/kevinswiber/boardwalk/commit/87d1ae0e1a4fbe53df471d977aa1711a56d15272)) - [@kevinswiber](https://github.com/kevinswiber)
- widen resource model and introduce resource/actor runtime traits - ([b0869e6](https://github.com/kevinswiber/boardwalk/commit/b0869e66dfc355cad301d8f711c4cd2c4c156523)) - [@kevinswiber](https://github.com/kevinswiber)
- event envelope, bounded subscriptions, and stream-gap protocol - ([017793b](https://github.com/kevinswiber/boardwalk/commit/017793bce285a76fa171465a5628537c8d80a1a5)) - [@kevinswiber](https://github.com/kevinswiber)
- contains + exists grammar; drop eval shims - ([35e42c8](https://github.com/kevinswiber/boardwalk/commit/35e42c83d4755c657eee23c406be11f8ad2d1d7d)) - [@kevinswiber](https://github.com/kevinswiber)
- route HTTP and app queries through ResourceSnapshot - ([857f268](https://github.com/kevinswiber/boardwalk/commit/857f2688eb350a2bd1399312173071591265210e)) - [@kevinswiber](https://github.com/kevinswiber)
- add ResourceSnapshot projection + DeviceSnapshot adapter - ([a8b4613](https://github.com/kevinswiber/boardwalk/commit/a8b4613c30ead8b15b097038e45b639e7750d69b)) - [@kevinswiber](https://github.com/kevinswiber)
- add runtime-owned query AST and evaluator - ([5916369](https://github.com/kevinswiber/boardwalk/commit/59163699706f001d7f197fcd01f6859eb444ca7c)) - [@kevinswiber](https://github.com/kevinswiber)
- proc-macro #[device], observe loop, tunnel cancellation - ([b8ec553](https://github.com/kevinswiber/boardwalk/commit/b8ec55320131468469cfcacd7706995663d72848)) - [@kevinswiber](https://github.com/kevinswiber)
- factories, scouts, observe, graceful shutdown, TLS test - ([3d2a4a7](https://github.com/kevinswiber/boardwalk/commit/3d2a4a7247d1e42be8a42515dd1ac961805b5d71)) - [@kevinswiber](https://github.com/kevinswiber)
- dedup peer subscriptions, persist devices, add apps - ([9f6926d](https://github.com/kevinswiber/boardwalk/commit/9f6926dd5cae7a3c5fba8be87d1bd489b59fbe3e)) - [@kevinswiber](https://github.com/kevinswiber)
- use rustls-platform-verifier for peer TLS - ([8efc220](https://github.com/kevinswiber/boardwalk/commit/8efc2205cd4a9a8e49c78f954cbf6836b790ff1c)) - [@kevinswiber](https://github.com/kevinswiber)
- forward peer queries and events through tunnel - ([dd04f04](https://github.com/kevinswiber/boardwalk/commit/dd04f04813185ca8a13a2d6e439774e5a6fa1207)) - [@kevinswiber](https://github.com/kevinswiber)
- implement v0 single-server runtime and peer handshake - ([a6ee938](https://github.com/kevinswiber/boardwalk/commit/a6ee938f7865c25ad323912947164edd2f9bb703)) - [@kevinswiber](https://github.com/kevinswiber)
#### Bug Fixes
- harden job runner event route contracts - ([09adadd](https://github.com/kevinswiber/boardwalk/commit/09adadd2cde3e67cf30c07fb7e6d8abbf2126bc7)) - [@kevinswiber](https://github.com/kevinswiber)
- keep unavailable actors visible in resource metadata - ([055c1dd](https://github.com/kevinswiber/boardwalk/commit/055c1dde899ec3a1a138f2440f219f689c45c719)) - [@kevinswiber](https://github.com/kevinswiber)
- tighten runtime contract surface - ([7b3ce49](https://github.com/kevinswiber/boardwalk/commit/7b3ce49e2f657b686a23825d9cef444daec06c42)) - [@kevinswiber](https://github.com/kevinswiber)
- harden job runner example - ([54e0eef](https://github.com/kevinswiber/boardwalk/commit/54e0eef9588e8455a8254f44911eaf95da9abcc9)) - [@kevinswiber](https://github.com/kevinswiber)
- resolve boardwalk path via proc-macro-crate and forward cfgs - ([4c5538f](https://github.com/kevinswiber/boardwalk/commit/4c5538f6edf5896d69ff5297b650e3b445c91477)) - [@kevinswiber](https://github.com/kevinswiber)
- keep job runner exemplar internal - ([25edd61](https://github.com/kevinswiber/boardwalk/commit/25edd614c14c30e04402daeebb5dc36e12c53300)) - [@kevinswiber](https://github.com/kevinswiber)
- harden job runner lifecycle contracts - ([f6356c2](https://github.com/kevinswiber/boardwalk/commit/f6356c2c58fc66046960fcbb425081a928fbdd34)) - [@kevinswiber](https://github.com/kevinswiber)
- drop resource type alias - ([3c50ad9](https://github.com/kevinswiber/boardwalk/commit/3c50ad97c9c4dc4b83c928d5b2c49d37a100a17d)) - [@kevinswiber](https://github.com/kevinswiber)
- address resource route review findings - ([585237f](https://github.com/kevinswiber/boardwalk/commit/585237f48ada2f699bc9f2c6241ae512da6e42eb)) - [@kevinswiber](https://github.com/kevinswiber)
- align metadata renderer with effect terminology - ([551c1dd](https://github.com/kevinswiber/boardwalk/commit/551c1dd1b826c2659a15b89320762466f4dfe851)) - [@kevinswiber](https://github.com/kevinswiber)
- harden CoalesceState against close race, missing keys, and receiver drop - ([f0ae81c](https://github.com/kevinswiber/boardwalk/commit/f0ae81c06b62f65104851918ef22f141fa95134c)) - [@kevinswiber](https://github.com/kevinswiber)
- make EventBus::publish cancellation-safe with an RAII claim guard - ([882e4c1](https://github.com/kevinswiber/boardwalk/commit/882e4c185fd896318cbbe7ec754ba3d3af6de39d)) - [@kevinswiber](https://github.com/kevinswiber)
- count concurrent publishes in flight to avoid premature subscription removal - ([c3030b5](https://github.com/kevinswiber/boardwalk/commit/c3030b56e8c4606814997fd9e567468a75185d0a)) - [@kevinswiber](https://github.com/kevinswiber)
- refund publish quota on lossy drop and preserve allowed_states on transition - ([b6f8311](https://github.com/kevinswiber/boardwalk/commit/b6f8311a16bd60cc72782a67db021006a57ff800)) - [@kevinswiber](https://github.com/kevinswiber)
- claim subscription slot under lock and populate allowed_states from when() - ([f11e759](https://github.com/kevinswiber/boardwalk/commit/f11e759ca21142c90b3bac4f194d100965ed9a49)) - [@kevinswiber](https://github.com/kevinswiber)
- address review findings on resource/actor runtime - ([d09c62d](https://github.com/kevinswiber/boardwalk/commit/d09c62d3c4d7c365e3442368645b9ce474c2ef2f)) - [@kevinswiber](https://github.com/kevinswiber)
- meta type-level transitions; topic filter URL syntax; nightly fmt - ([2200552](https://github.com/kevinswiber/boardwalk/commit/22005524532a14f608ec32ceb3b0bd1c257587e5)) - [@kevinswiber](https://github.com/kevinswiber)
- point Siren rel namespace at the registered boardwalk.to domain - ([5bb39e1](https://github.com/kevinswiber/boardwalk/commit/5bb39e19e12d37c1c54a087c132940958547f9c9)) - [@kevinswiber](https://github.com/kevinswiber)
#### Documentation
- finalize actor runtime docs and guards - ([ac6f646](https://github.com/kevinswiber/boardwalk/commit/ac6f646d587625e89860b7b3423e7f263f870c3f)) - [@kevinswiber](https://github.com/kevinswiber)
- document use_actor - ([4c9890e](https://github.com/kevinswiber/boardwalk/commit/4c9890e064ec54a8cb2c0ef5d5653f096ec3b65f)) - [@kevinswiber](https://github.com/kevinswiber)
- update resource actor vocabulary - ([7e255fb](https://github.com/kevinswiber/boardwalk/commit/7e255fb9cdba120aa64900a7404ec4f6ac9c4456)) - [@kevinswiber](https://github.com/kevinswiber)
- rewrite slow-consumer doc-comment without internal phase numbering - ([3dec052](https://github.com/kevinswiber/boardwalk/commit/3dec0529219944215f874bcf6910c4109995f32e)) - [@kevinswiber](https://github.com/kevinswiber)
- use boardwalk::transitions! in README quick start - ([c1fcb57](https://github.com/kevinswiber/boardwalk/commit/c1fcb5747f7d44ee5bfe6d7ae747eac3755b627e)) - [@kevinswiber](https://github.com/kevinswiber)
- refresh caql module rustdoc and README module table - ([d2eafe2](https://github.com/kevinswiber/boardwalk/commit/d2eafe2b052fe1a21e6fb910f0be6f873b623c03)) - [@kevinswiber](https://github.com/kevinswiber)
- cover contains/exists, kind alias, and ResourceSnapshot direction - ([4b538d4](https://github.com/kevinswiber/boardwalk/commit/4b538d4f9d4e358ce3d978a9c3e6442f911783c3)) - [@kevinswiber](https://github.com/kevinswiber)
- replace internal design notes with user-facing docs - ([59b9f29](https://github.com/kevinswiber/boardwalk/commit/59b9f29f179e279030a078a3b12fa113fcdaf6b2)) - [@kevinswiber](https://github.com/kevinswiber)
#### Refactoring
- remove legacy resource adapter - ([035ac6b](https://github.com/kevinswiber/boardwalk/commit/035ac6bb66c1d508db66577a529e9cd1c8bf0c45)) - [@kevinswiber](https://github.com/kevinswiber)
- move runtime contract types - ([d7d215f](https://github.com/kevinswiber/boardwalk/commit/d7d215f6bd9bb3431c38d9ea2d4587eeb445da5a)) - [@kevinswiber](https://github.com/kevinswiber)
- delete device public surface - ([0df25ea](https://github.com/kevinswiber/boardwalk/commit/0df25eafc3ea515599e5b16124c1206151fd3d7b)) - [@kevinswiber](https://github.com/kevinswiber)
- rename slow-consumer stream policy - ([3125578](https://github.com/kevinswiber/boardwalk/commit/3125578cf9e8d781bc3dd91861ea3165765fa8f4)) - [@kevinswiber](https://github.com/kevinswiber)
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) rename Safety to Effect and clarify the axis it models - ([7d45367](https://github.com/kevinswiber/boardwalk/commit/7d45367c08a40c8ff5b13e19fb8e534b580a925d)) - [@kevinswiber](https://github.com/kevinswiber)
- remove magic state-change auto-publish from actor executor - ([70e371c](https://github.com/kevinswiber/boardwalk/commit/70e371c44981137110a2642f845d91a180bba327)) - [@kevinswiber](https://github.com/kevinswiber)
- render through ResourceSnapshot - ([c147166](https://github.com/kevinswiber/boardwalk/commit/c147166b131744f645bbf7d14ccf8aef3d7913ed)) - [@kevinswiber](https://github.com/kevinswiber)
- parse to query::Query and delegate eval to query module - ([12b09cc](https://github.com/kevinswiber/boardwalk/commit/12b09cc41553a41e2c4c605bdce4366086ac55cd)) - [@kevinswiber](https://github.com/kevinswiber)
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) collapse multi-crate workspace into single boardwalk crate - ([50e948d](https://github.com/kevinswiber/boardwalk/commit/50e948de61afb371bef4e5167cb3c10845e725da)) - [@kevinswiber](https://github.com/kevinswiber)
- rename project from zetta to boardwalk - ([915b274](https://github.com/kevinswiber/boardwalk/commit/915b274cf70334413a4fdfb79e3886669ea93663)) - [@kevinswiber](https://github.com/kevinswiber)

- - -

