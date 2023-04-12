using System.Diagnostics;
using Microsoft.EntityFrameworkCore;

var options = new DbContextOptionsBuilder()
    .UseSqlServer("Server=.;Database=TestDb;Integrated Security=true;TrustServerCertificate=true;")
    .LogTo(Console.WriteLine)
    .Options;

long functionQueryDuration;
long windowQueryDuration;

using (var dbContext = new MyDbContext(options))
{
    var functionQuery =
        from partner in dbContext.BusinessPartners
        from order in dbContext.GetNewestOrders(partner.Id)
        where partner.Id > 270000 && partner.Id < 280000
        select new
        {
            PartnerId = partner.Id,
            OrderId = order.Id
        };

    var watcher = Stopwatch.StartNew();
    var result = functionQuery.ToList();
    functionQueryDuration = watcher.ElapsedMilliseconds;
    watcher.Stop();
    watcher.Reset();

    var windowQuery =
        from partner in dbContext.BusinessPartners
        from orderId in dbContext.Orders
            .Where(x => x.BusinessPartnerId == partner.Id)
            .OrderByDescending(x => x.CreatedOn)
            .Take(1)
            .Select(x => x.Id)
        where partner.Id > 270000 && partner.Id < 280000
        select new
        {
            PartnerId = partner.Id,
            OrderId = orderId
        };

    watcher.Start();
    var result2 = windowQuery.ToList();
    windowQueryDuration = watcher.ElapsedMilliseconds;
    watcher.Stop();
}



Console.WriteLine("______________________");
Console.WriteLine($"functionQuery: {functionQueryDuration}ms");
Console.WriteLine($"windowQuery: {windowQueryDuration}ms");

public class MyDbContext : DbContext
{
    public MyDbContext(DbContextOptions options)
        : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDbFunction(() => GetNewestOrders(default))
            .HasName("fn_GetNewestOrders");
    }
    
    public IQueryable<NewestOrderFunctionResultType> GetNewestOrders(long partnerId)
    {
        return FromExpression(() => GetNewestOrders(partnerId));
    }

    public DbSet<BusinessPartner> BusinessPartners { get; set; }
    public DbSet<Order> Orders { get; set; }
}

public class BusinessPartner
{
    public long Id { get; set; }

    public ICollection<Order> Orders { get; set; }
}

public class Order
{
    public long Id { get; set; }
    public long BusinessPartnerId { get; set; }
    public DateTime CreatedOn { get; set; }

    public BusinessPartner BusinessPartner { get; set; }
}

public class NewestOrderFunctionResultType
{
    public long Id { get; set; }
    public DateTime CreatedOn { get; set; }
}