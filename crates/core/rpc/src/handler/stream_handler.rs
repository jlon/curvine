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

use crate::handler::{Frame, MessageHandler};
use crate::message::{Builder, Message};
use crate::ServerConf;
use curvine_io::IOResult;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use log::debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::timeout;

// Network channel message processor. It associates network connection and message processing logic.
pub struct StreamHandler<F, M> {
    rt: Arc<Runtime>,
    frame: F,
    handler: Arc<M>,
    close_idle: bool,
    timeout: Duration,
    request_admission: Option<Arc<Semaphore>>,
}

impl<F: Frame, M: MessageHandler> StreamHandler<F, M> {
    pub fn new(rt: Arc<Runtime>, frame: F, handler: M, conf: &ServerConf) -> Self {
        Self::new_with_optional_admission(rt, frame, handler, conf, None)
    }

    pub fn new_with_request_admission(
        rt: Arc<Runtime>,
        frame: F,
        handler: M,
        conf: &ServerConf,
        request_admission: Arc<Semaphore>,
    ) -> Self {
        Self::new_with_optional_admission(rt, frame, handler, conf, Some(request_admission))
    }

    fn new_with_optional_admission(
        rt: Arc<Runtime>,
        frame: F,
        handler: M,
        conf: &ServerConf,
        request_admission: Option<Arc<Semaphore>>,
    ) -> Self {
        StreamHandler {
            rt,
            frame,
            handler: Arc::new(handler),
            close_idle: conf.close_idle,
            timeout: Duration::from_millis(conf.timeout_ms),
            request_admission,
        }
    }

    pub async fn run(&mut self) -> IOResult<()> {
        loop {
            let res = timeout(self.timeout, self.frame.receive()).await;
            let res = match res {
                Ok(v) => v,

                Err(_) if self.close_idle => {
                    // Close the timeout connection
                    return Ok(());
                }

                _ => continue,
            };

            match res {
                Ok(request) => {
                    if request.is_empty() {
                        return Ok(());
                    }

                    self.call(request).await?;
                }

                Err(e) => return Err(e),
            };
        }
    }

    pub async fn call(&mut self, request: Message) -> IOResult<()> {
        let admission = self
            .request_admission
            .clone()
            .or_else(|| self.handler.request_admission(&request));
        let response = if let Some(admission) = admission {
            let permit = admission
                .acquire_owned()
                .await
                .map_err(|_| "RPC request admission has stopped")?;
            let response = self.handle_request(request).await?;
            drop(permit);
            response
        } else {
            self.handle_request(request).await?
        };

        if response.not_empty() {
            self.frame.send(response).await
        } else {
            Ok(())
        }
    }

    async fn handle_request(&self, request: Message) -> IOResult<Message> {
        if self.handler.is_sync(&request) {
            let rt = self.handler.get_rt(&request).unwrap_or(&self.rt);

            let handler = self.handler.clone();
            Ok(rt
                .spawn_blocking(move || match handler.handle(&request) {
                    Err(e) => {
                        debug!("handler request {} error: {}", request.req_id(), e);
                        request.error_ext(&e)
                    }

                    Ok(v) => v,
                })
                .await?)
        } else {
            let protocol = request.protocol;
            match self.handler.async_handle(request).await {
                Ok(v) => Ok(v),
                Err(e) => {
                    debug!("handler request {} error: {}", protocol.req_id, e);
                    Ok(Builder::protocol(protocol).build().error_ext(&e))
                }
            }
        }
    }

    pub fn frame_mut(&mut self) -> &mut F {
        &mut self.frame
    }
}
