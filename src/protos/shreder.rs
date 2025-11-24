// Shreder protocol protobuf definitions
use prost_types::Timestamp;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeTransactionsRequest {
    #[prost(map = "string, message", tag = "3")]
    pub transactions: std::collections::HashMap<String, SubscribeRequestFilterTransactions>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeTransactionsResponse {
    #[prost(string, repeated, tag = "1")]
    pub filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "4")]
    pub transaction: ::core::option::Option<SubscribeUpdateTransaction>,
    #[prost(message, optional, tag = "11")]
    pub created_at: ::core::option::Option<Timestamp>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeUpdateTransaction {
    #[prost(message, optional, tag = "1")]
    pub transaction: ::core::option::Option<Transaction>,
    #[prost(uint64, tag = "2")]
    pub slot: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeRequestFilterTransactions {
    #[prost(string, repeated, tag = "3")]
    pub account_include: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "4")]
    pub account_exclude: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "6")]
    pub account_required: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessageHeader {
    #[prost(uint32, tag = "1")]
    pub num_required_signatures: u32,
    #[prost(uint32, tag = "2")]
    pub num_readonly_signed_accounts: u32,
    #[prost(uint32, tag = "3")]
    pub num_readonly_unsigned_accounts: u32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompiledInstruction {
    #[prost(uint32, tag = "1")]
    pub program_id_index: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub accounts: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessageAddressTableLookup {
    #[prost(bytes = "vec", tag = "1")]
    pub account_key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub writable_indexes: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub readonly_indexes: ::prost::alloc::vec::Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Message {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<MessageHeader>,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub account_keys: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    #[prost(bytes = "vec", tag = "3")]
    pub recent_blockhash: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "4")]
    pub instructions: ::prost::alloc::vec::Vec<CompiledInstruction>,
    #[prost(bool, tag = "5")]
    pub versioned: bool,
    #[prost(message, repeated, tag = "6")]
    pub address_table_lookups: ::prost::alloc::vec::Vec<MessageAddressTableLookup>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transaction {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub signatures: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    #[prost(message, optional, tag = "2")]
    pub message: ::core::option::Option<Message>,
}

/// Generated client implementations.
pub mod shreder_service_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;

    #[derive(Debug, Clone)]
    pub struct ShrederServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }

    impl ShrederServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }

    impl<T> ShrederServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }

        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }

        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ShrederServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::Body>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::Body>,
            >>::Error: Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            ShrederServiceClient::new(InterceptedService::new(inner, interceptor))
        }

        /// Compress requests with the given encoding.
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }

        /// Enable decompressing responses.
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }

        /// Limits the maximum size of a decoded message.
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }

        /// Limits the maximum size of an encoded message.
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }

        pub async fn subscribe_transactions(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::SubscribeTransactionsRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::SubscribeTransactionsResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|_e| {
                    tonic::Status::unknown("Service was not ready")
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/shreder.ShrederService/SubscribeTransactions",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("shreder.ShrederService", "SubscribeTransactions"),
                );
            self.inner.streaming(req, path, codec).await
        }
    }
}
