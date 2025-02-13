using System.Collections.Concurrent;
using System.Text;
using NumType = float;

var start = DateTime.Now;

var stations = new ConcurrentDictionary<string, Station>();

const string folder = @"C:\Users\Kyle\Documents\source\OdinOneBillionRows\data";
const string filename =
    //"measurements-1_000.txt";
    //"measurements-1_000_000.txt";
    "measurements.txt";
const string filepath = $"{folder}\\{filename}";

using (FileStream fs = new FileStream(filepath, FileMode.Open, FileAccess.Read, FileShare.Read))
using (StreamReader reader = new StreamReader(fs, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 4096, leaveOpen: true))
{
    const int chunkSize = 250;

    while (true)
    {
        if (reader.EndOfStream) break;

        var chunk = new List<string>(chunkSize);
        for (int i = 0; i < chunkSize; i++)
        {
            var line = reader.ReadLine();            
            if (line is not null) chunk.Add(line);
            if (reader.EndOfStream) break;
        }

        if (chunk.Count() == 0) break;

        ThreadPool.QueueUserWorkItem(
            ProcessChunk
            , new ChunkContext(chunk, ref stations)
        );
    }
}

bool first = true;
var ittr = stations.Values.OrderBy(x => x.Name);
foreach (var st in ittr)
{
    if (!first)
        Console.Write(",");
    else
        first = false;

    Console.Write(st.ToString());
}
Console.Write("\n");

var end = DateTime.Now;
var runtime = end - start;
Console.WriteLine($"Runtime: {runtime}");

return 0;



static void ProcessChunk(object obj)
{
    var ctx = (ChunkContext)obj;
    foreach (var line in ctx.Lines)
    {
        ProcessLine(ref ctx, line);
    }
}

static void ProcessLine(ref ChunkContext ctx, string line)
{
    var bits = line.Split(';');
    if (bits.Length != 2) return;

    var name = bits[0];
    var vStr = bits[1];
    var vNum = NumType.Parse(vStr);

    var station = ctx.Stations.GetOrAdd(name, (key) => new Station(key));
    station.Update(vNum);
}

class ChunkContext(IEnumerable<string> lines, ref ConcurrentDictionary<string, Station> stations)
{
    public IEnumerable<string> Lines = lines;
    public ConcurrentDictionary<string, Station> Stations = stations;
};

class Station
{
    public string Name;
    public uint Count;
    public NumType Sum;
    public NumType? Min;
    public NumType? Max;

    public NumType GetAvg() => Sum / Count;
    public override string ToString() => $"{Name}/{Count}/{Min}/{Max}/{GetAvg()}";

    public Station(string name)
    {
        Name = name;
        Count = 0;
        Sum = 0;
        Min = null;
        Max = null;
    }

    public void Update(NumType value)
    {
        Count += 1;
        Sum += value;
        if (Min is null || value < Min) Min = value;
        if (Max is null || value > Max) Max = value;
    }
}

// ;
// while (true)
// {
//     var chunk = file.Read()



//     var line = file.ReadLine();
//     if (line is not null && line?.Length > 0)
//     {
//         ThreadPool.QueueUserWorkItem(ProcessLine, new WorkerContext(line, stations));
//     }
//     if (file.EndOfStream)
//     {
//         break;
//     }
// }

// bool first = false;
// foreach (var st in stations.Values)
// {
//     if (!first)
//         Console.Write(",");
//     else
//         first = false;

//     Console.Write(st.ToString());
// }
// Console.Write("\n");

// var end = DateTime.Now;
// var runtime = end - start;
// Console.WriteLine($"Runtime: {runtime}");

// static void ProcessLine(object? obj)
// {
//     var ctx = (WorkerContext)obj;

//     var bits = ctx.Line.Split(';');
//     if (bits.Length != 2) return;

//     var name = bits[0];
//     var vStr = bits[1];
//     var vNum = NumType.Parse(vStr);

//     var station = ctx.Stations.GetOrAdd(name, (key) => new Station(key));
//     station.Update(vNum);
// }


// class WorkerContext(string line, ConcurrentDictionary<string, Station> stations)
// {
//     public string Line = line;
//     public ConcurrentDictionary<string, Station> Stations = stations;
// };