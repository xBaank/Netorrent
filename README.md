# Netorrent
Implementation of [BiTorrent Protocol](https://bittorrent.org/beps/bep_0003.html)

## Supported
- [x] Torrent files
- [x] Http Trackers
- [X] Peer Wire Protocol (TCP)
- [ ] [μTP](https://www.bittorrent.org/beps/bep_0029.html)
- [ ] [Udp trackers](https://www.bittorrent.org/beps/bep_0015.html)
- [ ] Upnp & pmp (Allows incoming TCP connections when the peer is behind a nat)
- [ ] Udp hole punching (Requires μTP implemented)
- [ ] Magnet Links
- [ ] Endgame (High priority)

## TODO
- [ ] Improve chocking/unchoking
- [ ] Improve memory handling and cpu usage with MemoryRented
- [ ] Properly handle peers that lost connection to connect to other peers 
