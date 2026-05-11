// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::time::Duration;

use crate::curvine_database::CurvineIntegrityDatabase;
use crate::object_store::curvine_session;
use lancedb_upstream::connection::ConnectBuilder as UpstreamConnectBuilder;
use lancedb_upstream::database::DatabaseOptions;
use lancedb_upstream::embeddings::{EmbeddingRegistry, MemoryRegistry};
#[cfg(feature = "remote")]
use lancedb_upstream::remote::ClientConfig;
use lancedb_upstream::{
    connect as upstream_connect, connect_namespace as upstream_connect_namespace,
    Result as UpstreamResult, Session,
};

pub use lancedb_upstream::connection::{CloneTableBuilder, OpenTableBuilder, TableNamesBuilder};
pub use lancedb_upstream::connection::{ConnectRequest, Connection, LanceFileVersion};

#[derive(Debug)]
enum ConnectBuilderInner {
    Upstream(Box<UpstreamConnectBuilder>),
    Curvine {
        uri: String,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<Duration>,
        embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
        session: Option<Arc<Session>>,
    },
}

#[derive(Debug)]
pub struct ConnectBuilder {
    inner: ConnectBuilderInner,
}

impl ConnectBuilder {
    pub fn new(uri: &str) -> Self {
        if is_curvine_uri(uri) {
            Self {
                inner: ConnectBuilderInner::Curvine {
                    uri: uri.to_string(),
                    storage_options: HashMap::new(),
                    read_consistency_interval: None,
                    embedding_registry: None,
                    session: Some(curvine_session()),
                },
            }
        } else {
            let builder = upstream_connect(uri);
            Self {
                inner: ConnectBuilderInner::Upstream(Box::new(builder)),
            }
        }
    }

    pub fn database_options(self, database_options: &dyn DatabaseOptions) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(
                    builder.database_options(database_options),
                )),
            },
            ConnectBuilderInner::Curvine {
                uri,
                mut storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
            } => {
                database_options.serialize_into_map(&mut storage_options);
                Self {
                    inner: ConnectBuilderInner::Curvine {
                        uri,
                        storage_options,
                        read_consistency_interval,
                        embedding_registry,
                        session,
                    },
                }
            }
        }
    }

    pub fn embedding_registry(self, registry: Arc<dyn EmbeddingRegistry>) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(
                    builder.embedding_registry(registry),
                )),
            },
            ConnectBuilderInner::Curvine {
                uri,
                storage_options,
                read_consistency_interval,
                session,
                ..
            } => Self {
                inner: ConnectBuilderInner::Curvine {
                    uri,
                    storage_options,
                    read_consistency_interval,
                    embedding_registry: Some(registry),
                    session,
                },
            },
        }
    }

    pub fn storage_option(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(builder.storage_option(key, value))),
            },
            ConnectBuilderInner::Curvine {
                uri,
                mut storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
            } => {
                storage_options.insert(key, value);
                Self {
                    inner: ConnectBuilderInner::Curvine {
                        uri,
                        storage_options,
                        read_consistency_interval,
                        embedding_registry,
                        session,
                    },
                }
            }
        }
    }

    pub fn storage_options(
        self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(builder.storage_options(pairs))),
            },
            ConnectBuilderInner::Curvine {
                uri,
                mut storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
            } => {
                for (key, value) in pairs {
                    storage_options.insert(key.into(), value.into());
                }
                Self {
                    inner: ConnectBuilderInner::Curvine {
                        uri,
                        storage_options,
                        read_consistency_interval,
                        embedding_registry,
                        session,
                    },
                }
            }
        }
    }

    pub fn read_consistency_interval(self, read_consistency_interval: Duration) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(
                    builder.read_consistency_interval(read_consistency_interval),
                )),
            },
            ConnectBuilderInner::Curvine {
                uri,
                storage_options,
                embedding_registry,
                session,
                ..
            } => Self {
                inner: ConnectBuilderInner::Curvine {
                    uri,
                    storage_options,
                    read_consistency_interval: Some(read_consistency_interval),
                    embedding_registry,
                    session,
                },
            },
        }
    }

    pub fn session(self, session: Arc<Session>) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(builder.session(session))),
            },
            ConnectBuilderInner::Curvine {
                uri,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                ..
            } => Self {
                inner: ConnectBuilderInner::Curvine {
                    uri,
                    storage_options,
                    read_consistency_interval,
                    embedding_registry,
                    session: Some(session),
                },
            },
        }
    }

    #[cfg(feature = "remote")]
    pub fn api_key(self, api_key: &str) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(builder.api_key(api_key))),
            },
            curvine @ ConnectBuilderInner::Curvine { .. } => Self { inner: curvine },
        }
    }

    #[cfg(feature = "remote")]
    pub fn region(self, region: &str) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(builder.region(region))),
            },
            curvine @ ConnectBuilderInner::Curvine { .. } => Self { inner: curvine },
        }
    }

    #[cfg(feature = "remote")]
    pub fn host_override(self, host_override: &str) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(
                    builder.host_override(host_override),
                )),
            },
            curvine @ ConnectBuilderInner::Curvine { .. } => Self { inner: curvine },
        }
    }

    #[cfg(feature = "remote")]
    pub fn client_config(self, config: ClientConfig) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(builder.client_config(config))),
            },
            curvine @ ConnectBuilderInner::Curvine { .. } => Self { inner: curvine },
        }
    }

    pub async fn execute(self) -> UpstreamResult<Connection> {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => builder.execute().await,
            ConnectBuilderInner::Curvine {
                uri,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
            } => {
                let db = CurvineIntegrityDatabase::connect(
                    &uri,
                    storage_options,
                    read_consistency_interval,
                    session,
                )
                .await?;
                Ok(Connection::new(
                    Arc::new(db),
                    embedding_registry.unwrap_or_else(|| Arc::new(MemoryRegistry::new())),
                ))
            }
        }
    }
}

pub fn connect(uri: &str) -> ConnectBuilder {
    ConnectBuilder::new(uri)
}

enum ConnectNamespaceBuilderInner {
    Pending {
        ns_impl: String,
        properties: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<Duration>,
        embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
        session: Option<Arc<Session>>,
        server_side_query: bool,
    },
}

pub struct ConnectNamespaceBuilder {
    inner: ConnectNamespaceBuilderInner,
}

impl Debug for ConnectNamespaceBuilderInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query,
            } => f
                .debug_struct("Pending")
                .field("ns_impl", ns_impl)
                .field("properties", properties)
                .field("storage_options", storage_options)
                .field("read_consistency_interval", read_consistency_interval)
                .field("embedding_registry", &embedding_registry.is_some())
                .field("session", &session.is_some())
                .field("server_side_query", server_side_query)
                .finish(),
        }
    }
}

impl Debug for ConnectNamespaceBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ConnectNamespaceBuilder")
            .field("inner", &self.inner)
            .finish()
    }
}

impl ConnectNamespaceBuilder {
    fn new(ns_impl: &str, properties: HashMap<String, String>) -> Self {
        Self {
            inner: ConnectNamespaceBuilderInner::Pending {
                ns_impl: ns_impl.to_string(),
                properties,
                storage_options: HashMap::new(),
                read_consistency_interval: None,
                embedding_registry: None,
                session: None,
                server_side_query: false,
            },
        }
    }

    fn map_upstream(
        self,
        f: impl FnOnce(ConnectNamespaceBuilderInner) -> ConnectNamespaceBuilderInner,
    ) -> Self {
        Self {
            inner: f(self.inner),
        }
    }

    pub fn storage_option(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                mut storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query,
            } => {
                storage_options.insert(key.into(), value.into());
                ConnectNamespaceBuilderInner::Pending {
                    ns_impl,
                    properties,
                    storage_options,
                    read_consistency_interval,
                    embedding_registry,
                    session,
                    server_side_query,
                }
            }
        })
    }

    pub fn storage_options(
        self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                mut storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query,
            } => {
                for (key, value) in pairs {
                    storage_options.insert(key.into(), value.into());
                }
                ConnectNamespaceBuilderInner::Pending {
                    ns_impl,
                    properties,
                    storage_options,
                    read_consistency_interval,
                    embedding_registry,
                    session,
                    server_side_query,
                }
            }
        })
    }

    pub fn read_consistency_interval(self, read_consistency_interval: Duration) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                embedding_registry,
                session,
                server_side_query,
                ..
            } => ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval: Some(read_consistency_interval),
                embedding_registry,
                session,
                server_side_query,
            },
        })
    }

    pub fn embedding_registry(self, registry: Arc<dyn EmbeddingRegistry>) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                session,
                server_side_query,
                ..
            } => ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry: Some(registry),
                session,
                server_side_query,
            },
        })
    }

    pub fn session(self, session: Arc<Session>) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                server_side_query,
                ..
            } => ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session: Some(session),
                server_side_query,
            },
        })
    }

    pub fn server_side_query(self, enabled: bool) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                ..
            } => ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query: enabled,
            },
        })
    }

    pub async fn execute(self) -> UpstreamResult<Connection> {
        match self.inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query,
            } => {
                let wants_curvine = find_curvine_uri(&properties);
                if let Some(uri) = wants_curvine {
                    let db = CurvineIntegrityDatabase::connect(
                        &uri,
                        storage_options,
                        read_consistency_interval,
                        session.or_else(|| Some(curvine_session())),
                    )
                    .await?;
                    let _ = (ns_impl, server_side_query);
                    return Ok(Connection::new(
                        Arc::new(db),
                        embedding_registry.unwrap_or_else(|| Arc::new(MemoryRegistry::new())),
                    ));
                }

                let mut builder = upstream_connect_namespace(&ns_impl, properties);

                for (key, value) in storage_options {
                    builder = builder.storage_option(key, value);
                }

                if let Some(read_consistency_interval) = read_consistency_interval {
                    builder = builder.read_consistency_interval(read_consistency_interval);
                }

                if let Some(embedding_registry) = embedding_registry {
                    builder = builder.embedding_registry(embedding_registry);
                }

                if let Some(session) = session {
                    builder = builder.session(session);
                }

                builder.server_side_query(server_side_query).execute().await
            }
        }
    }
}

pub fn connect_namespace(
    ns_impl: &str,
    properties: HashMap<String, String>,
) -> ConnectNamespaceBuilder {
    ConnectNamespaceBuilder::new(ns_impl, properties)
}

fn is_curvine_uri(uri: &str) -> bool {
    uri.starts_with("curvine://")
}

fn find_curvine_uri(properties: &HashMap<String, String>) -> Option<String> {
    for key in ["root", "uri"] {
        if let Some(value) = properties.get(key) {
            if is_curvine_uri(value) {
                return Some(value.clone());
            }
        }
    }

    properties
        .values()
        .find(|value| is_curvine_uri(value))
        .cloned()
}
