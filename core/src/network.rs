use steel::*;
#[cfg(not(feature = "solana"))]
use core::net::{SocketAddr, Ipv4Addr, Ipv6Addr};

#[derive(Error, Debug, PartialEq, Eq)]
pub enum NetworkAddressError {
    #[error("Invalid address format")]
    InvalidAddressFormat,
}

/// A network address in a zeroable/pod type.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetworkAddress {
    data: [u8; 24],
}

unsafe impl Pod for NetworkAddress {}
unsafe impl Zeroable for NetworkAddress {}

impl Default for NetworkAddress {
    #[inline]
    fn default() -> Self {
        Self { data: [0; 24] }
    }
}

impl NetworkAddress {
    #[inline]
    pub fn new(flags: u16, port_le: u16, ip: [u8; 16]) -> Self {
        let mut na = Self::default();
        na.set_flags(flags);
        na.set_port_le(port_le);
        na.set_ip(ip);
        na
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 24] { &self.data }

    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8; 24] { &mut self.data }

    #[inline]
    pub fn from_bytes(bytes: [u8; 24]) -> Self { Self { data: bytes } }

    #[inline]
    pub fn into_bytes(self) -> [u8; 24] { self.data }

    #[inline]
    pub fn flags(&self) -> u16 {
        u16::from_le_bytes(self.data[0..2].try_into().unwrap())
    }
    #[inline]
    pub fn set_flags(&mut self, v: u16) {
        self.data[0..2].copy_from_slice(&v.to_le_bytes());
    }

    #[inline]
    fn port_le(&self) -> u16 {
        u16::from_le_bytes(self.data[2..4].try_into().unwrap())
    }
    #[inline]
    fn set_port_le(&mut self, v: u16) {
        self.data[2..4].copy_from_slice(&v.to_le_bytes());
    }

    #[inline]
    pub fn port(&self) -> u16 { self.port_le() }
    #[inline]
    pub fn set_port(&mut self, v: u16) { self.set_port_le(v) }

    #[inline]
    pub fn ip(&self) -> [u8; 16] {
        self.data[4..20].try_into().unwrap()
    }
    #[inline]
    pub fn set_ip(&mut self, ip: [u8; 16]) {
        self.data[4..20].copy_from_slice(&ip);
    }

    #[inline]
    pub fn is_ipv4(&self) -> bool { self.flags() == 0 }
    #[inline]
    pub fn is_ipv6(&self) -> bool { self.flags() == 1 }

    #[cfg(not(feature = "solana"))]
    pub fn from(addr: &str) -> Result<Self, NetworkAddressError> {
        match addr.parse::<SocketAddr>() {
            Ok(sa) => Ok(Self::from_socket_addr(sa)),
            Err(_) => Err(NetworkAddressError::InvalidAddressFormat),
        }
    }

    #[cfg(not(feature = "solana"))]
    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => {
                let mut ip = [0u8; 16];
                ip[..4].copy_from_slice(&v4.ip().octets());
                Self::new(0, v4.port().to_le(), ip)
            }
            SocketAddr::V6(v6) => {
                let ip = v6.ip().octets();
                Self::new(1, v6.port().to_le(), ip)
            }
        }
    }

    #[cfg(not(feature = "solana"))]
    pub fn to_socket_addr(&self) -> Result<SocketAddr, &'static str> {
        let port = u16::from_le(self.port_le());
        match self.flags() {
            0 => {
                let ipb = self.ip();
                let ip = Ipv4Addr::new(ipb[0], ipb[1], ipb[2], ipb[3]);
                Ok(SocketAddr::V4(std::net::SocketAddrV4::new(ip, port)))
            }
            1 => {
                let ip = Ipv6Addr::from(self.ip());
                Ok(SocketAddr::V6(std::net::SocketAddrV6::new(ip, port, 0, 0)))
            }
            _ => Err("Invalid flags value"),
        }
    }
}

#[cfg(not(feature = "solana"))]
impl From<SocketAddr> for NetworkAddress {
    fn from(sa: SocketAddr) -> Self { Self::from_socket_addr(sa) }
}

impl AsRef<[u8]> for NetworkAddress {
    fn as_ref(&self) -> &[u8] { &self.data }
}

impl AsMut<[u8]> for NetworkAddress {
    fn as_mut(&mut self) -> &mut [u8] { &mut self.data }
}


#[cfg(test)]
#[cfg(not(feature = "solana"))]
mod tests {
    use super::*;
    use bytemuck::{cast_slice, try_from_bytes};
    use core::net::{Ipv6Addr, SocketAddr};

    #[test]
    fn test_ipv4() {
        let addr_str = "192.168.1.1:12345";
        let addr: SocketAddr = addr_str.parse().unwrap();
        let network_addr = NetworkAddress::from(addr_str).unwrap();

        assert_eq!(network_addr.flags(), 0);
        assert_eq!(network_addr.port(), 12345u16.to_le());
        assert_eq!(network_addr.ip()[..4], [192, 168, 1, 1]);
        assert_eq!(network_addr.ip()[4..], [0; 12]);

        let data = &[network_addr];
        let bytes = cast_slice(data);
        let deserialized: &NetworkAddress = try_from_bytes(bytes).unwrap();
        let restored_addr = deserialized.to_socket_addr().unwrap();

        assert_eq!(restored_addr, addr);
    }

    #[test]
    fn test_ipv6() {
        let addr: SocketAddr = "[2001:db8::8:800:200c:417a]:8081".parse().unwrap();
        let network_addr = NetworkAddress::from_socket_addr(addr);

        assert_eq!(network_addr.flags(), 1);
        assert_eq!(network_addr.port(), 8081u16.to_le());
        assert_eq!(
            network_addr.ip(),
            Ipv6Addr::from([
                0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x08, 0x08, 0x00, 0x20, 0x0c, 0x41, 0x7a
            ]).octets()
        );

        let data = &[network_addr];
        let bytes = cast_slice(data);
        let deserialized: &NetworkAddress = try_from_bytes(bytes).unwrap();
        let restored_addr = deserialized.to_socket_addr().unwrap();

        assert_eq!(restored_addr, addr);
    }
}
