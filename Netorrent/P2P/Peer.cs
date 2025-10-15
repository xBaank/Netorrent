using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Netorrent.P2P;

public record Peer(IPAddress IP, int Port);
