use bytemuck::{Pod, Zeroable};
use core::net::{SocketAddr, Ipv4Addr, Ipv6Addr};
use crate::define_u64_type;

define_u64_type!(TAPE, "coin");
define_u64_type!(VersionNumber, "version");
define_u64_type!(EpochNumber, "epoch");
define_u64_type!(PoolNumber, "pool");
define_u64_type!(ArchiveNumber, "archive");
define_u64_type!(SpoolNumber, "spool");
define_u64_type!(BasisPoints, "bps");

/// A type alias for coin amounts.
pub type Coin<T> = T;

/// A generic ring buffer to hold entries of type `T`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RingBuffer<T: Pod + Zeroable, const N: usize> {
    pub index: u64,
    pub length: u64,
    pub entries: [T; N],
}

unsafe impl<T: Pod + Zeroable, const N: usize> Zeroable for RingBuffer<T, N> {}
unsafe impl<T: Pod + Zeroable, const N: usize> Pod for RingBuffer<T, N> {}

impl<T: Pod + Zeroable, const N: usize> RingBuffer<T, N> {
    /// Returns true if the buffer has no entries.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns true if the buffer is full.
    pub fn is_full(&self) -> bool {
        self.length as usize == N
    }

    /// Returns the current number of entries.
    pub fn len(&self) -> usize {
        self.length as usize
    }

    /// Returns the maximum capacity.
    pub fn capacity(&self) -> usize {
        N
    }

    /// Push a new entry into the ring buffer.
    /// If full, overwrites the oldest entry.
    pub fn push(&mut self, entry: T) {
        let idx = (self.index + self.length) % N as u64;
        self.entries[idx as usize] = entry;

        if self.is_full() {
            // Overwrite: advance the start
            self.index = (self.index + 1) % N as u64;
        } else {
            self.length += 1;
        }
    }

    /// Returns a reference to the most recent entry, if any.
    pub fn back(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            let idx = (self.index + self.length - 1) % N as u64;
            Some(&self.entries[idx as usize])
        }
    }

    /// Returns a reference to the oldest entry, if any.
    pub fn front(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            Some(&self.entries[self.index as usize])
        }
    }

    /// Get an entry by relative index (0 = oldest).
    pub fn get(&self, i: usize) -> Option<&T> {
        if i >= self.len() {
            None
        } else {
            let idx = (self.index + i as u64) % N as u64;
            Some(&self.entries[idx as usize])
        }
    }

    /// Iterate over entries in order from oldest to newest.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        (0..self.len()).map(move |i| {
            let idx = (self.index + i as u64) % N as u64;
            &self.entries[idx as usize]
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum NetworkAddressError {
    InvalidAddressFormat,
}

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

    pub fn from(addr: &str) -> Result<Self, NetworkAddressError> {
        match addr.parse::<SocketAddr>() {
            Ok(sa) => Ok(Self::from_socket_addr(sa)),
            Err(_) => Err(NetworkAddressError::InvalidAddressFormat),
        }
    }

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
mod tests {
    use super::*;
    use bytemuck::{cast_slice, try_from_bytes};
    use core::net::{Ipv6Addr, SocketAddr};

    #[test]
    fn test_ipv4_from_string_and_back() {
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
    fn test_ipv6_from_socket_addr_and_bytes() {
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
