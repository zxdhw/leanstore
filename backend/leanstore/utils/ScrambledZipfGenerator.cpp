#include "ScrambledZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
u64 ScrambledZipfGenerator::rand()
{
   u64 zipf_value = zipf_generator.rand();
   // std:: cout << "zipf_value: " << zipf_value << std::endl;
   return min + (FNV::hash(zipf_value) % n);
}
u64 ScrambledZipfGenerator::rand_nohash()
{
   u64 zipf_value = zipf_generator.rand();
   // std:: cout << "zipf_value: " << zipf_value << std::endl;
   return zipf_value;
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
