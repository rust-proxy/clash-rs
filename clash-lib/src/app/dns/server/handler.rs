use super::DEFAULT_DNS_SERVER_TTL;
use crate::app::dns::{ThreadSafeDNSResolver, helper::build_dns_response_message};
use hickory_proto::{
    op::{Message, ResponseCode},
    rr::{RData, Record, RecordType, rdata::A},
};
use tracing::debug;

pub async fn exchange_with_resolver<'a>(
    resolver: &'a ThreadSafeDNSResolver,
    req: &'a Message,
    enhanced: bool,
) -> Result<Message, watfaq_dns::DNSError> {
    let query = req
        .queries
        .first()
        .ok_or(watfaq_dns::DNSError::InvalidOpQuery(
            "malformed query message".to_string(),
        ))?;
    let fake = enhanced && resolver.fake_ip_enabled();
    if fake
        && matches!(
            query.query_type(),
            RecordType::AAAA | RecordType::HTTPS | RecordType::SVCB
        )
        && let Some(response) = resolver.fake_ip_ipv6_response(req).await
    {
        return Ok(response);
    }
    if !fake || query.query_type() != RecordType::A {
        return match resolver.exchange(req).await {
            Ok(m) => Ok(m),
            Err(e) => {
                debug!("dns resolve error: {}", e);
                Err(watfaq_dns::DNSError::QueryFailed(e.to_string()))
            }
        };
    }

    let name = query.name().clone();
    let fqdn = name.to_ascii();
    let host = fqdn.trim_end_matches('.');

    let mut res = build_dns_response_message(req, false, false);

    match resolver.resolve_v4(host, enhanced).await {
        Ok(resp) => match resp {
            Some(ip) => {
                let rdata = RData::A(A(ip));

                let records =
                    vec![Record::from_rdata(name, DEFAULT_DNS_SERVER_TTL, rdata)];

                res.metadata.response_code = ResponseCode::NoError;
                res.add_answers(records);

                Ok(res)
            }
            None => Ok(res),
        },
        Err(e) => {
            debug!("dns resolve error: {}", e);
            Err(watfaq_dns::DNSError::QueryFailed(e.to_string()))
        }
    }
}
