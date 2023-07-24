using PlatformService.Models;

namespace PlatformService.Data
{
  public interface IPlatformRepo
  {
    bool SaveChanges();

    IEnumerable<Platform> GetAllPlatforms();
    Platform GetPlaformById(int id);
    void CreatePlatform(Platform plat);
  }
}