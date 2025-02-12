using NumType = float;

var start = DateTime.Now;

const string folder = @"C:\Users\Kyle\Documents\source\OdinOneBillionRows\data";
const string filename = 
    //"measurements-1_000.txt";
    "measurements-1_000_000.txt";
    //"measurements.txt";
const string filepath = $"{folder}\\{filename}";
using var file = new StreamReader(filepath);

var stations = new Dictionary<string, Station>();

while (true)
{
    var line = file.ReadLine();
    if (line is not null && line?.Length > 0)
    {
        ProcessLine(line);
    }
    if (file.EndOfStream)
    {
        break;
    }
}

var end = DateTime.Now;
var runtime = end - start;

bool first = false;
foreach (var st in stations.Values)
{
    if (!first)
        Console.Write(",");
    else
        first = false;

    Console.Write(st.ToString());
}
Console.Write("\n");

Console.WriteLine($"Runtime: {runtime}");


void ProcessLine(string line)
{
    var bits = line.Split(';');
    if (bits.Length != 2) return;

    var name = bits[0];
    var vStr = bits[1];
    var vNum = NumType.Parse(vStr);

    var existing = stations.GetValueOrDefault(name);
    if (existing is {})
    {
        existing.Update(vNum);
    }
    else
    {
        stations.Add(name, new Station(name, vNum));
    }
}

class Station
{
    public string Name;
    public uint Count;
    public NumType Sum;
    public NumType Min;
    public NumType Max;

    public NumType GetAvg() => Sum / Count;
    public override string ToString() => $"{Name}/{Count}/{Min}/{Max}/{GetAvg()}";

    public Station(string name, NumType value)
    {
        Name = name;
        Count = 1;
        Sum = value;
        Min = value;
        Max = value;
    }

    public void Update(NumType value)
    {
        Count += 1;
        Sum += value;
        if (value < Min) Min = value;
        if (value > Max) Max = value;
    }
}