use bytemuck::{Pod, Zeroable};
use core::net::{SocketAddr, Ipv4Addr, Ipv6Addr};
use crate::define_u64_type;

define_u64_type!(TAPE, "coin");

pub type Coin<T> = T;
pub type Balance<T> = T;


#[derive(Debug, PartialEq)]
pub enum NetworkAddressError {
    InvalidAddressFormat,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct NetworkAddress {
    pub flags: u16,
    pub port: u16,
    pub ip: [u8; 16],
    pub _empty: u32,
}

impl NetworkAddress {
    pub fn default() -> Self {
        NetworkAddress { flags: 0, port: 0, ip: [0; 16], _empty: 0, }
    }

    pub fn from(addr: &str) -> Result<Self, NetworkAddressError> {
        match addr.parse::<SocketAddr>() {
            Ok(socket_addr) => Ok(Self::from_socket_addr(socket_addr)),
            Err(_) => Err(NetworkAddressError::InvalidAddressFormat),
        }
    }

    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => {
                let ip = v4.ip().octets();
                let mut ip_bytes = [0u8; 16];
                ip_bytes[..4].copy_from_slice(&ip);
                NetworkAddress {
                    flags: 0,
                    port: v4.port().to_le(),
                    ip: ip_bytes,
                    _empty: 0,
                }
            }
            SocketAddr::V6(v6) => NetworkAddress {
                flags: 1,
                port: v6.port().to_le(),
                ip: v6.ip().octets(),
                _empty: 0,
            },
        }
    }

    pub fn to_socket_addr(&self) -> Result<SocketAddr, &'static str> {
        let port = u16::from_le(self.port); // Convert from little-endian
        if self.flags == 0 {
            let ip_bytes = &self.ip[..4];
            let ip = Ipv4Addr::new(ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3]);
            Ok(SocketAddr::V4(std::net::SocketAddrV4::new(ip, port)))
        } else if self.flags == 1 {
            let ip = Ipv6Addr::from(self.ip);
            Ok(SocketAddr::V6(std::net::SocketAddrV6::new(ip, port, 0, 0)))
        } else {
            Err("Invalid flags value")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::{cast_slice, try_from_bytes};
    use core::net::{Ipv6Addr, SocketAddr};

    #[test]
    fn test_round_trip_ipv4() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let network_addr = NetworkAddress::from_socket_addr(addr);

        assert_eq!(network_addr.flags, 0);
        assert_eq!(network_addr.port, 8080u16.to_le());
        assert_eq!(network_addr.ip[..4], [127, 0, 0, 1]);
        assert_eq!(network_addr.ip[4..], [0; 12]);

        let data = &[network_addr];
        let bytes = cast_slice(data);
        let deserialized: &NetworkAddress = try_from_bytes(bytes).unwrap();
        let restored_addr = deserialized.to_socket_addr().unwrap();

        assert_eq!(restored_addr, addr);
    }

    #[test]
    fn test_round_trip_ipv6() {
        let addr: SocketAddr = "[2001:db8::1:2:3:4]:8080".parse().unwrap();
        let network_addr = NetworkAddress::from_socket_addr(addr);
        
        assert_eq!(network_addr.flags, 1);
        assert_eq!(network_addr.port, 8080u16.to_le());
        assert_eq!(
            network_addr.ip,
            Ipv6Addr::from([
                0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04
            ]).octets()
        );

        let data = &[network_addr];
        let bytes = cast_slice(data);
        let deserialized: &NetworkAddress = try_from_bytes(bytes).unwrap();
        let restored_addr = deserialized.to_socket_addr().unwrap();

        assert_eq!(restored_addr, addr);
    }
}
