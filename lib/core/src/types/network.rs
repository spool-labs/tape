#[cfg(not(target_os = "solana"))]
use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use core::str;

use bytemuck::{Pod, Zeroable};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

pub const MAX_DOMAIN_LEN: usize = 60;

const KIND_OFFSET:    usize = 0;
const LENGTH_OFFSET:  usize = 1;
const PORT_OFFSET:    usize = 2;
const PAYLOAD_OFFSET: usize = 4;
const PORT_LEN:       usize = 2;
const IPV4_LEN:       usize = 4;
const IPV6_LEN:       usize = 16;

const KIND_IPV4:      u8 = 0;
const KIND_IPV6:      u8 = 1;
const KIND_DOMAIN:    u8 = 2;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddressKind {
    Ipv4 = KIND_IPV4,
    Ipv6 = KIND_IPV6,
    Domain = KIND_DOMAIN,
}

/// A network address in a zeroable/pod type.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct NetworkAddress {
    data: [u8; 64],
}

unsafe impl Pod for NetworkAddress {}
unsafe impl Zeroable for NetworkAddress {}

impl Default for NetworkAddress {
    #[inline]
    fn default() -> Self {
        Self::new_ipv4([0; IPV4_LEN], 0)
    }
}

impl AddressKind {
    #[inline]
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            KIND_IPV4 => Some(Self::Ipv4),
            KIND_IPV6 => Some(Self::Ipv6),
            KIND_DOMAIN => Some(Self::Domain),
            _ => None,
        }
    }
}

impl NetworkAddress {
    #[inline]
    fn new(kind: AddressKind, payload: &[u8], port: u16) -> Self {
        let mut address = Self {
            data: [0; 64],
        };

        address.set_kind(kind);
        address.set_payload_len(payload.len() as u8);
        address.set_port(port);
        address.data[PAYLOAD_OFFSET..PAYLOAD_OFFSET + payload.len()].copy_from_slice(payload);
        address
    }

    #[inline]
    pub fn new_ipv4(ip: [u8; IPV4_LEN], port: u16) -> Self {
        Self::new(AddressKind::Ipv4, &ip, port)
    }

    #[inline]
    pub fn new_ipv6(ip: [u8; IPV6_LEN], port: u16) -> Self {
        Self::new(AddressKind::Ipv6, &ip, port)
    }

    pub fn new_domain(host: &str, port: u16) -> Result<Self, NetworkAddressError> {
        if port == 0 {
            return Err(NetworkAddressError::PortZero);
        }

        let bytes = host.as_bytes();
        if bytes.is_empty() {
            return Err(NetworkAddressError::DomainEmpty);
        }
        if bytes.len() > MAX_DOMAIN_LEN {
            return Err(NetworkAddressError::DomainTooLong);
        }
        if bytes.iter().any(|byte| !is_domain_byte(*byte)) {
            return Err(NetworkAddressError::InvalidDomainCharacter);
        }

        Ok(Self::new(AddressKind::Domain, bytes, port))
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 64] { &self.data }

    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8; 64] { &mut self.data }

    #[inline]
    pub fn from_bytes(bytes: [u8; 64]) -> Self { Self { data: bytes } }

    #[inline]
    pub fn into_bytes(self) -> [u8; 64] { self.data }

    #[inline]
    pub fn kind(&self) -> AddressKind {
        AddressKind::from_byte(self.kind_byte()).unwrap_or(AddressKind::Ipv4)
    }

    #[inline]
    fn kind_byte(&self) -> u8 { self.data[KIND_OFFSET] }

    #[inline]
    fn set_kind(&mut self, kind: AddressKind) {
        self.data[KIND_OFFSET] = kind as u8;
    }

    #[inline]
    fn payload_len(&self) -> u8 { self.data[LENGTH_OFFSET] }

    #[inline]
    fn set_payload_len(&mut self, len: u8) {
        self.data[LENGTH_OFFSET] = len;
    }

    #[inline]
    pub fn port(&self) -> u16 {
        u16::from_le_bytes([
            self.data[PORT_OFFSET],
            self.data[PORT_OFFSET + PORT_LEN - 1],
        ])
    }

    #[inline]
    pub fn set_port(&mut self, v: u16) {
        self.data[PORT_OFFSET..PORT_OFFSET + PORT_LEN].copy_from_slice(&v.to_le_bytes());
    }

    pub fn ip(&self) -> Option<[u8; IPV6_LEN]> {
        match self.kind_byte() {
            KIND_IPV4 => {
                let mut ip = [0; IPV6_LEN];
                ip[..IPV4_LEN].copy_from_slice(&self.data[PAYLOAD_OFFSET..PAYLOAD_OFFSET + IPV4_LEN]);
                Some(ip)
            }
            KIND_IPV6 => {
                let mut ip = [0; IPV6_LEN];
                ip.copy_from_slice(&self.data[PAYLOAD_OFFSET..PAYLOAD_OFFSET + IPV6_LEN]);
                Some(ip)
            }
            _ => None,
        }
    }

    pub fn domain(&self) -> Option<&str> {
        if self.kind_byte() != KIND_DOMAIN {
            return None;
        }

        let len = self.payload_len() as usize;
        if !is_domain_len(len) {
            return None;
        }

        let payload = &self.data[PAYLOAD_OFFSET..PAYLOAD_OFFSET + len];
        if payload.iter().any(|byte| byte.is_ascii_control()) {
            return None;
        }

        str::from_utf8(payload).ok()
    }

    #[inline]
    pub fn is_ipv4(&self) -> bool { self.kind_byte() == KIND_IPV4 }

    #[inline]
    pub fn is_ipv6(&self) -> bool { self.kind_byte() == KIND_IPV6 }

    #[inline]
    pub fn is_domain(&self) -> bool { self.kind_byte() == KIND_DOMAIN }

    pub fn validate(&self) -> Result<(), NetworkAddressError> {
        let len = self.payload_len() as usize;
        match self.kind_byte() {
            KIND_IPV4 if len == IPV4_LEN => self.validate_padding(len),
            KIND_IPV6 if len == IPV6_LEN => self.validate_padding(len),
            KIND_DOMAIN if is_domain_len(len) => {
                let payload = &self.data[PAYLOAD_OFFSET..PAYLOAD_OFFSET + len];
                if payload.iter().any(|byte| byte.is_ascii_control()) {
                    return Err(NetworkAddressError::InvalidDomainPayload);
                }
                self.validate_padding(len)
            }
            KIND_IPV4 | KIND_IPV6 | KIND_DOMAIN => Err(NetworkAddressError::InvalidLength),
            _ => Err(NetworkAddressError::InvalidKind),
        }
    }

    fn validate_padding(&self, len: usize) -> Result<(), NetworkAddressError> {
        if self.data[PAYLOAD_OFFSET + len..]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(NetworkAddressError::NonCanonicalPadding);
        }
        Ok(())
    }

    #[cfg(not(target_os = "solana"))]
    pub fn authority(&self) -> Result<String, NetworkAddressError> {
        self.validate()?;

        match self.kind_byte() {
            KIND_IPV4 => {
                let ip = self.ip().ok_or(NetworkAddressError::InvalidKind)?;
                Ok(format!(
                    "{}.{}.{}.{}:{}",
                    ip[0],
                    ip[1],
                    ip[2],
                    ip[3],
                    self.port()
                ))
            }
            KIND_IPV6 => {
                let ip = self.ip().ok_or(NetworkAddressError::InvalidKind)?;
                Ok(format!("[{}]:{}", Ipv6Addr::from(ip), self.port()))
            }
            KIND_DOMAIN => {
                let host = self
                    .domain()
                    .ok_or(NetworkAddressError::InvalidDomainPayload)?;
                Ok(format!("{host}:{}", self.port()))
            }
            _ => Err(NetworkAddressError::InvalidKind),
        }
    }

    #[cfg(not(target_os = "solana"))]
    pub fn from(addr: &str) -> Result<Self, NetworkAddressError> {
        if let Ok(socket_address) = addr.parse::<SocketAddr>() {
            if socket_address.port() == 0 {
                return Err(NetworkAddressError::PortZero);
            }
            return Ok(Self::from_socket_addr(socket_address));
        }

        let (host, port) = addr
            .rsplit_once(':')
            .ok_or(NetworkAddressError::MissingPort)?;
        if host.is_empty() {
            return Err(NetworkAddressError::InvalidAddressFormat);
        }
        let port = port
            .parse::<u16>()
            .map_err(|_| NetworkAddressError::InvalidPort)?;

        Self::new_domain(host, port)
    }

    #[cfg(not(target_os = "solana"))]
    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => Self::new_ipv4(v4.ip().octets(), v4.port()),
            SocketAddr::V6(v6) => Self::new_ipv6(v6.ip().octets(), v6.port()),
        }
    }

    #[cfg(not(target_os = "solana"))]
    pub fn to_socket_addr(&self) -> Result<SocketAddr, NetworkAddressError> {
        self.validate()?;

        match self.kind_byte() {
            KIND_IPV4 => {
                let ip = self.ip().ok_or(NetworkAddressError::InvalidKind)?;
                Ok(SocketAddr::new(
                    Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]).into(),
                    self.port(),
                ))
            }
            KIND_IPV6 => {
                let ip = self.ip().ok_or(NetworkAddressError::InvalidKind)?;
                Ok(SocketAddr::new(Ipv6Addr::from(ip).into(), self.port()))
            }
            KIND_DOMAIN => Err(NetworkAddressError::DomainSocketAddressUnsupported),
            _ => Err(NetworkAddressError::InvalidKind),
        }
    }
}

#[cfg(not(target_os = "solana"))]
impl From<SocketAddr> for NetworkAddress {
    fn from(socket_address: SocketAddr) -> Self { Self::from_socket_addr(socket_address) }
}

impl AsRef<[u8]> for NetworkAddress {
    fn as_ref(&self) -> &[u8] { &self.data }
}

impl AsMut<[u8]> for NetworkAddress {
    fn as_mut(&mut self) -> &mut [u8] { &mut self.data }
}

#[inline]
fn is_domain_len(len: usize) -> bool { (1..=MAX_DOMAIN_LEN).contains(&len) }

#[inline]
fn is_domain_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-'
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkAddressError {
    InvalidAddressFormat,
    MissingPort,
    InvalidPort,
    PortZero,
    DomainEmpty,
    DomainTooLong,
    InvalidDomainCharacter,
    InvalidKind,
    InvalidLength,
    InvalidDomainPayload,
    NonCanonicalPadding,
    DomainSocketAddressUnsupported,
}

impl core::fmt::Display for NetworkAddressError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let message = match self {
            Self::InvalidAddressFormat => "invalid network address format",
            Self::MissingPort => "network address must include a port",
            Self::InvalidPort => "network address port is invalid",
            Self::PortZero => "network address port must be non-zero",
            Self::DomainEmpty => "domain name must not be empty",
            Self::DomainTooLong => "domain name exceeds 60 bytes",
            Self::InvalidDomainCharacter => "domain name may only contain letters, digits, dots, and hyphens",
            Self::InvalidKind => "network address kind is invalid",
            Self::InvalidLength => "network address payload length is invalid",
            Self::InvalidDomainPayload => "network address domain payload is invalid",
            Self::NonCanonicalPadding => "network address padding must be zero",
            Self::DomainSocketAddressUnsupported => "domain network address cannot be converted to SocketAddr without DNS resolution"
        };
        formatter.write_str(message)
    }
}

#[cfg(not(target_os = "solana"))]
impl std::error::Error for NetworkAddressError {}

#[cfg(test)]
#[cfg(not(target_os = "solana"))]
mod tests {
    use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

    use bytemuck::{cast_slice, try_from_bytes};

    use super::*;

    #[test]
    fn ipv4_roundtrip() {
        let addr: SocketAddr = "192.168.1.1:12345".parse().expect("socket addr");
        let network_addr = NetworkAddress::from("192.168.1.1:12345").expect("network addr");

        assert_eq!(network_addr.kind(), AddressKind::Ipv4);
        assert!(network_addr.is_ipv4());
        assert_eq!(network_addr.port(), 12345);
        assert_eq!(network_addr.ip().expect("ip")[..IPV4_LEN], [192, 168, 1, 1]);
        assert_eq!(network_addr.to_socket_addr().expect("socket addr"), addr);

        let values = [network_addr];
        let bytes = cast_slice(&values);
        let deserialized: &NetworkAddress = try_from_bytes(bytes).expect("deserialize");
        assert_eq!(deserialized.to_socket_addr().expect("socket addr"), addr);
    }

    #[test]
    fn ipv6_roundtrip() {
        let addr: SocketAddr = "[2001:db8::8:800:200c:417a]:8081"
            .parse()
            .expect("socket addr");
        let network_addr = NetworkAddress::from_socket_addr(addr);

        assert_eq!(network_addr.kind(), AddressKind::Ipv6);
        assert!(network_addr.is_ipv6());
        assert_eq!(network_addr.port(), 8081);
        assert_eq!(
            network_addr.ip().expect("ip"),
            Ipv6Addr::from([
                0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x08, 0x08, 0x00, 0x20, 0x0c, 0x41, 0x7a,
            ])
            .octets()
        );

        let values = [network_addr];
        let bytes = cast_slice(&values);
        let deserialized: &NetworkAddress = try_from_bytes(bytes).expect("deserialize");
        assert_eq!(deserialized.to_socket_addr().expect("socket addr"), addr);
    }

    #[test]
    fn domain_roundtrip() {
        let network_addr =
            NetworkAddress::from("node07.devnet.tape.network:3430").expect("network addr");

        assert_eq!(network_addr.kind(), AddressKind::Domain);
        assert!(network_addr.is_domain());
        assert_eq!(network_addr.port(), 3430);
        assert_eq!(network_addr.domain(), Some("node07.devnet.tape.network"));
        assert!(network_addr.to_socket_addr().is_err());

        let values = [network_addr];
        let bytes = cast_slice(&values);
        let deserialized: &NetworkAddress = try_from_bytes(bytes).expect("deserialize");
        assert_eq!(
            deserialized.domain(),
            Some("node07.devnet.tape.network")
        );
    }

    #[test]
    fn rejects_bad_domain() {
        let long = "a".repeat(MAX_DOMAIN_LEN + 1);
        assert_eq!(
            NetworkAddress::new_domain(&long, 443),
            Err(NetworkAddressError::DomainTooLong)
        );
        assert_eq!(
            NetworkAddress::new_domain("bad host", 443),
            Err(NetworkAddressError::InvalidDomainCharacter)
        );
        assert_eq!(
            NetworkAddress::new_domain("https://example.com", 443),
            Err(NetworkAddressError::InvalidDomainCharacter)
        );
        assert_eq!(
            NetworkAddress::new_domain("example.com", 0),
            Err(NetworkAddressError::PortZero)
        );
    }

    #[test]
    fn validate_rejects_malformed() {
        let mut bytes = NetworkAddress::new_ipv4([127, 0, 0, 1], 443).into_bytes();

        bytes[KIND_OFFSET] = 9;
        assert_eq!(
            NetworkAddress::from_bytes(bytes).validate(),
            Err(NetworkAddressError::InvalidKind)
        );

        let mut bytes = NetworkAddress::new_ipv4([127, 0, 0, 1], 443).into_bytes();
        bytes[LENGTH_OFFSET] = IPV6_LEN as u8;
        assert_eq!(
            NetworkAddress::from_bytes(bytes).validate(),
            Err(NetworkAddressError::InvalidLength)
        );

        let mut bytes = NetworkAddress::new_domain("example.com", 443)
            .expect("domain")
            .into_bytes();
        bytes[PAYLOAD_OFFSET + 3] = 0;
        assert_eq!(
            NetworkAddress::from_bytes(bytes).validate(),
            Err(NetworkAddressError::InvalidDomainPayload)
        );

        let mut bytes = NetworkAddress::new_ipv4([127, 0, 0, 1], 443).into_bytes();
        bytes[PAYLOAD_OFFSET + IPV4_LEN] = 1;
        assert_eq!(
            NetworkAddress::from_bytes(bytes).validate(),
            Err(NetworkAddressError::NonCanonicalPadding)
        );

        let mut bytes = NetworkAddress::new_domain("example.com", 443)
            .expect("domain")
            .into_bytes();
        bytes[PAYLOAD_OFFSET + "example.com".len()] = 1;
        assert_eq!(
            NetworkAddress::from_bytes(bytes).validate(),
            Err(NetworkAddressError::NonCanonicalPadding)
        );
    }

    #[test]
    fn authority_formats_variants() {
        let ipv4 = NetworkAddress::new_ipv4(Ipv4Addr::new(1, 2, 3, 4).octets(), 443);
        let ipv6 = NetworkAddress::new_ipv6(Ipv6Addr::LOCALHOST.octets(), 8443);
        let domain = NetworkAddress::new_domain("node07.devnet.tape.network", 3430)
            .expect("domain");

        assert_eq!(ipv4.authority().expect("authority"), "1.2.3.4:443");
        assert_eq!(ipv6.authority().expect("authority"), "[::1]:8443");
        assert_eq!(
            domain.authority().expect("authority"),
            "node07.devnet.tape.network:3430"
        );
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn wincode_roundtrip() {
        let network_addr = NetworkAddress::new_domain("node07.devnet.tape.network", 3430)
            .expect("domain");

        let bytes = wincode::serialize(&network_addr).expect("serialize");
        let decoded: NetworkAddress = wincode::deserialize(&bytes).expect("deserialize");

        assert_eq!(decoded, network_addr);
    }
}
